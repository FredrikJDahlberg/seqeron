using System;
using System.Collections.Generic;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Helpers;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Util;
using Xunit;

namespace Org.Limitless.Seqeron.Replayer.Client;

/// <summary>
/// <see cref="ReplayerRecovery"/> against a MODEL of the counterparty it talks to, rather than against scripted
/// stimuli: <see cref="ReplayerRecoveryTest"/> names one situation per case and asserts the decision taken in it,
/// but it can only reach the interleavings someone thought to write. Here the node's archive, its tap and its
/// Replayer are simulated, a seeded generator drives faults through them, and the two properties recovery exists to
/// provide are asserted over whatever comes out.
/// <para><b>Safety</b>: every dispatched frame is the next globalSeqNo, always, checked on every dispatch so a
/// violation names the frame that broke it.</para>
/// <para><b>Liveness</b>: once the faults stop, recovery converges: caught up, at the tip, having dispatched every
/// frame ever published.</para>
/// <para><b>Restore</b>: a client restoring its own snapshot (doc/snapshot.md §7) holds both properties from the cut
/// on, under the same faults, and its restored state plus the frames after the cut equals the state a full replay
/// builds. It holds files of three rounds, of which the log ends only the middle one: the newest gives way to
/// it.</para>
/// <para>The model draws from <see cref="SplitMix64"/> in the same order as <c>ReplayerRecoveryPropertyTest.java</c>
/// and its C++ twin, so a seed is the same fault sequence in all three. Seeds are fixed and listed: a failing run
/// must be re-runnable.</para>
/// </summary>
[Collection(LoggerCollection.Name)]
public class ReplayerRecoveryPropertyTest : IDisposable
{
    private const int ClientId = 4;

    // One frame's span in the recording's position space. Opaque to the client: only ordering matters.
    private const int Stride = 64;

    // Stands in for the wall clock. The absolute value is arbitrary; only applied deltas matter.
    private const long ClockMs = 3 * 60 * 60 * 1000L;

    // Arrival stamp; carried through to SequencedEvent, asserted on by no test here.
    private const long ReceiveNs = 0;

    private const int ChaosSteps = 600;

    // Generous: convergence takes a bounded number of steps, and a run that needs them all still passes.
    private const int QuiesceSteps = 20_000;

    // The restored source, and its snapshot: round 2, superseding round 1, cut at Cut.
    private const int Source = 3;
    private const int OtherSource = 5;
    private const long Cut = 9;
    private const long SnapshotEnd = 12;
    private const int FormatVersion = 1;

    private readonly TempDirectory _directory = new TempDirectory();

    // Every gap logs, and a run makes hundreds: kept out of the build output rather than counted.
    public ReplayerRecoveryPropertyTest()
    {
        Logger.Install(new CallbackLoggerSink(Discard));
    }

    private static void Discard(Logger.LoggerEvent loggerEvent)
    {
    }

    public void Dispose()
    {
        Logger.Reset();
        _directory.Dispose();
    }

    [Theory]
    [InlineData(1UL), InlineData(2UL), InlineData(3UL), InlineData(5UL), InlineData(8UL), InlineData(13UL)]
    [InlineData(21UL), InlineData(34UL), InlineData(55UL), InlineData(89UL), InlineData(144UL), InlineData(233UL)]
    [InlineData(377UL), InlineData(610UL), InlineData(987UL), InlineData(1597UL)]
    public void StaysGapFreeAndConvergesUnderRandomFaults(ulong seed)
    {
        new Run(seed, null).Execute();
    }

    [Theory]
    [InlineData(1UL), InlineData(2UL), InlineData(3UL), InlineData(5UL), InlineData(8UL), InlineData(13UL)]
    [InlineData(21UL), InlineData(34UL), InlineData(55UL), InlineData(89UL), InlineData(144UL), InlineData(233UL)]
    [InlineData(377UL), InlineData(610UL), InlineData(987UL), InlineData(1597UL)]
    public void RestoresAndConvergesToTheFullReplayStateUnderRandomFaults(ulong seed)
    {
        new Run(seed, new SnapshotStore(_directory.Path)).Execute();
    }

    /// <summary>
    /// One seeded run: the node's archive, its tap and its Replayer, driving one <see cref="ReplayerRecovery"/>. It
    /// implements the actions itself: the client's every outbound act is a request arriving at this Replayer, so
    /// recording them and serving them are the same object.
    /// </summary>
    private sealed class Run : IReplayerRecoveryActions, ISnapshotRestoreHandler
    {
        private readonly SplitMix64 _rng;
        private readonly string _tag;
        private readonly bool _restoring;

        // The client's own files, null for a run that restores nothing.
        private readonly SnapshotStore _store;

        // The digest of the one round the log ends for Source.
        private RestoreFrames.Digest _snapshotDigest;

        // Every frame that is not a heartbeat, by globalSeqNo: the snapshot rounds.
        private readonly Dictionary<long, byte[]> _frames = new Dictionary<long, byte[]>();

        // The state: how many heartbeats, and the sum of their globalSeqNos; restored, then folded.
        private long _heartbeats;
        private long _heartbeatSum;
        private int _nextRecordIndex;

        // ── the node's archive: the active recording, complete by construction ─────────────────────────
        private long _tip;

        // Where the active recording's frame 1 starts; a rotation may move it.
        private long _positionBase;

        // ── the Replayer's view of this one client ─────────────────────────────────────────────────────
        private long _pendingRequestId = -1;
        private bool _pendingIsQuery;
        private long _pendingQueryRound;
        private long _pendingFromPosition;
        private long _nextSessionId = 1;

        // Next globalSeqNo the open replay will deliver; _replayEndSeqNo < 0 means none is open.
        private long _replayCursor;
        private long _replayEndSeqNo = -1;

        // Last globalSeqNo the tap actually delivered: what a redelivery re-offers.
        private long _lastTapped;

        private long _clockMs = ClockMs;
        private bool _chaos = true;

        // The safety property, one frame at a time.
        private long _expectedNext = 1;

        private readonly UnsafeBuffer _wire = new UnsafeBuffer();
        private ReplayerRecovery _client;

        public Run(ulong seed, SnapshotStore store)
        {
            _rng = new SplitMix64(seed);
            _restoring = store != null;
            _tag = "seed=" + seed + (_restoring ? " restoring" : "");
            _store = store;
        }

        public void Execute()
        {
            // History before the client starts. A cold start over an EMPTY archive that then drops the very first
            // tap frame is the one designed abort (the first frame observed must be globalSeqNo 1) and not a
            // recovery failure, so the model does not construct it.
            if (_restoring)
            {
                PublishSnapshotRounds();
            }
            for (int i = 0; i < 3; ++i)
            {
                Publish();
            }

            _client = new ReplayerRecovery(ClientId, this, OnSequenced, null, null);
            if (_restoring)
            {
                _client.RestoreFrom(Source, _store, this);
                _expectedNext = Cut + 1;
            }
            _client.Start();

            for (int step = 0; step < ChaosSteps; ++step)
            {
                ChaosStep();
            }

            // Quiescence: faults off, but the tap keeps running. A hole the chaos phase left open is only ever
            // discovered by the NEXT tap frame, so convergence has to be given one.
            _chaos = false;
            for (int i = 0; i < 3; ++i)
            {
                DeliverTap(Publish());
            }
            for (int step = 0; step < QuiesceSteps && !Converged(); ++step)
            {
                if (_pendingRequestId >= 0)
                {
                    ServeRequest();
                }
                else if (_replayEndSeqNo >= 0)
                {
                    DeliverReplayFrames(4);
                }
                else
                {
                    Tick();
                }
            }

            Assert.True(_client.IsCaughtUp, _tag + ": never re-converged after the faults stopped");
            Assert.True(_tip == _client.LastGlobalSeqNo, _tag + ": converged short of the tip");
            Assert.True(_tip + 1 == _expectedNext, _tag + ": caught up without having dispatched every frame");
            if (_restoring)
            {
                long count = 0;
                long sum = 0;
                for (long globalSeqNo = 1; globalSeqNo <= _tip; ++globalSeqNo)
                {
                    if (!_frames.ContainsKey(globalSeqNo))
                    {
                        ++count;
                        sum += globalSeqNo;
                    }
                }
                Assert.True(count == _heartbeats, _tag + ": restored state plus the tail is not the full replay's");
                Assert.True(sum == _heartbeatSum, _tag + ": restored state plus the tail is not the full replay's");
            }
        }

        // Round 1 starts and is superseded by round 2 at Cut before its source ends it; round 2 ends among
        // heartbeats and another source's end, and round 3 starts and never ends. The client holds a file of each:
        // round 2's records hold the state at the cut, the heartbeats before it.
        private void PublishSnapshotRounds()
        {
            for (int i = 0; i < 5; ++i)
            {
                Publish(); // heartbeats 1-5
            }
            byte[] header = RestoreFrames.Header(4, 2);
            PublishFrame(RestoreFrames.Started(6, 1));
            Publish(); // heartbeat 7
            Publish(); // heartbeat 8
            PublishFrame(RestoreFrames.Started(Cut, 2));
            byte[] count = RestoreFrames.Record(7);
            byte[] sum = RestoreFrames.Record(1 + 2 + 3 + 4 + 5 + 7 + 8);
            PublishFrame(RestoreFrames.End(10, OtherSource, 2, FormatVersion, header));
            Publish(); // heartbeat 11
            PublishFrame(RestoreFrames.End(SnapshotEnd, Source, 2, FormatVersion, header, count, sum));
            PublishFrame(RestoreFrames.Started(13, 3));
            Publish(); // heartbeat 14

            _snapshotDigest = RestoreFrames.Digest.Of(header, count, sum);
            RestoreFrames.Write(_store, 1, FormatVersion, header, RestoreFrames.Record(99));
            RestoreFrames.Write(_store, 2, FormatVersion, header, count, sum);
            RestoreFrames.Write(_store, 3, FormatVersion, header, RestoreFrames.Record(98));
        }

        private void PublishFrame(byte[] frame)
        {
            _frames[Publish()] = frame;
        }

        private bool Converged()
        {
            return _client.IsCaughtUp && _client.LastGlobalSeqNo == _tip;
        }

        private void ChaosStep()
        {
            switch (_rng.Roll(10))
            {
                case 0:
                case 1:
                case 2:
                case 3:
                    long globalSeqNo = Publish();
                    // A tap drop is the fault the whole resume path exists for, so it is the common one.
                    if (!Chance(20))
                    {
                        DeliverTap(globalSeqNo);
                    }
                    break;
                case 4:
                case 5:
                    ServeRequest();
                    break;
                case 6:
                case 7:
                    DeliverReplayFrames(1 + _rng.Roll(3));
                    break;
                case 8:
                    Tick();
                    break;
                default:
                    InjectFault();
                    break;
            }
        }

        private void InjectFault()
        {
            switch (_rng.Roll(4))
            {
                case 0:
                    if (_replayEndSeqNo >= 0)
                    {
                        // The Replayer stopped this replay under us: its image closes SHORT of the bound.
                        _client.OnReplayImageClosed(PositionOf(_replayCursor));
                    }
                    break;
                case 1:
                    _clockMs += 6_000; // past ReplayStallTimeoutMs as well as the resend interval
                    _client.DoTimers(false);
                    break;
                case 2:
                    // The same tap frame offered twice. Both de-dupes have to hold: the contiguity one when it sits
                    // at or below the baseline, and the retained FIFO's when it is ahead of a hole.
                    if (_lastTapped > 0)
                    {
                        Deliver(_lastTapped, false);
                    }
                    break;
                default:
                    Rotate();
                    break;
            }
        }

        // A new active recording, holding the log from globalSeqNo 1 as a restarted node's does once it has replayed
        // it. A walk's may lay its frames out elsewhere (a frame or half a frame on), so a resume at an old position
        // opens on another frame or is refused; a restoring run's keeps its positions, since a resume at the
        // snapshot's position is its only way back.
        private void Rotate()
        {
            if (!_restoring)
            {
                _positionBase += _rng.Chance(50) ? Stride : Stride / 2;
            }
        }

        // ── the tap ────────────────────────────────────────────────────────────────────────────────────

        private long Publish()
        {
            return ++_tip;
        }

        private void DeliverTap(long globalSeqNo)
        {
            _lastTapped = globalSeqNo;
            Deliver(globalSeqNo, false);
        }

        private void Deliver(long globalSeqNo, bool fromReplay)
        {
            if (!_frames.TryGetValue(globalSeqNo, out byte[] frame))
            {
                frame = Frames.ClusterHeartbeat(globalSeqNo);
            }
            _wire.Wrap(frame);
            _client.OnFrame(_wire, 0, frame.Length, PositionOf(globalSeqNo), ReceiveNs, fromReplay);
        }

        // ── the Replayer ───────────────────────────────────────────────────────────────────────────────

        private void ServeRequest()
        {
            if (_pendingRequestId < 0)
            {
                return;
            }
            long requestId = _pendingRequestId;
            bool query = _pendingIsQuery;
            long queryRound = _pendingQueryRound;
            long fromPosition = _pendingFromPosition;
            _pendingRequestId = -1;
            if (requestId != _client.RequestId)
            {
                return; // superseded by a request this run dropped: the Replayer would serve the newer one
            }

            if (_chaos && Chance(10))
            {
                Control(ControlMessages.ReplayPending(ClientId, requestId));
                return;
            }
            if (_chaos && Chance(5))
            {
                Control(ControlMessages.ReplayUnavailable(ClientId, requestId));
                return;
            }
            if (query)
            {
                Control(queryRound == 2 ? RestoreFrames.Location(ClientId, requestId, 2, Cut, PositionOf(Cut),
                                                                 FormatVersion, _snapshotDigest)
                                        : RestoreFrames.Location(ClientId, requestId, -1, -1, -1, 0,
                                                                 new RestoreFrames.Digest(0, 0, 0)));
                return;
            }

            if (fromPosition == ReplayProtocol.FromStart)
            {
                ServeReplay(requestId, 1, _tip);
                return;
            }
            long offset = fromPosition - _positionBase;
            if (offset < 0 || offset % Stride != 0 || offset / Stride + 1 > _tip)
            {
                // Not a frame of the active recording: it rotated under the client, and the archive refuses it.
                Control(ControlMessages.Replaying(ClientId, requestId, ReplayProtocol.NoReplayNeeded, 0));
                return;
            }
            ServeReplay(requestId, offset / Stride + 1, _tip);
        }

        // Bound at the tip the recording holds NOW: frames published later are the tap's problem, not this
        // replay's, which is exactly how a bounded replay of an active recording behaves.
        private void ServeReplay(long requestId, long fromSeqNo, long toSeqNo)
        {
            _replayCursor = fromSeqNo;
            _replayEndSeqNo = toSeqNo;
            Control(ControlMessages.Replaying(ClientId, requestId, _nextSessionId++, PositionOf(toSeqNo + 1)));
        }

        // Feeds up to `count` frames off the open replay, then reports where it has reached. Both the frames and the
        // position report can make the client abandon the replay (an anchor mismatch, the bound being reached),
        // which CloseReplay records: hence the re-checks.
        private void DeliverReplayFrames(int count)
        {
            for (int i = 0; i < count && _replayEndSeqNo >= 0 && _replayCursor <= _replayEndSeqNo; ++i)
            {
                Deliver(_replayCursor++, true);
            }
            if (_replayEndSeqNo >= 0)
            {
                _client.OnReplayPosition(PositionOf(_replayCursor));
            }
        }

        private void Control(byte[] message)
        {
            _wire.Wrap(message);
            _client.OnControl(_wire, 0, message.Length);
        }

        private void Tick()
        {
            _clockMs += 100 + _rng.Roll(900); // straddles ResendIntervalMs
            _client.DoTimers(false);
            _client.CheckRecoveryProgress();
        }

        // ── IReplayerRecoveryActions: the client's outbound side ───────────────────────────────────────

        public void SendReplayRequest(long requestId, long fromPosition)
        {
            if (_chaos && Chance(15))
            {
                return; // the offer did not land; only the resend timer recovers this
            }
            _pendingRequestId = requestId;
            _pendingIsQuery = false;
            _pendingFromPosition = fromPosition;
        }

        public void SendSnapshotQuery(long requestId, int sourceId, long round)
        {
            Assert.True(sourceId == Source, _tag);
            Assert.True(round == 3 || round == 2, $"{_tag}: asked about round {round}");
            if (_chaos && Chance(15))
            {
                return;
            }
            _pendingRequestId = requestId;
            _pendingIsQuery = true;
            _pendingQueryRound = round;
        }

        public bool SendReplayComplete()
        {
            return !_chaos || Chance(70);
        }

        public bool SendReplayHeartbeat()
        {
            return !_chaos || Chance(70);
        }

        public void OpenReplay(long replaySessionId)
        {
        }

        public void CloseReplay()
        {
            _replayEndSeqNo = -1;
        }

        public void RecoveryStalled(bool stalled)
        {
        }

        public int? MemberId => 0;

        public long NowMs()
        {
            return _clockMs;
        }

        // ── the safety property ────────────────────────────────────────────────────────────────────────

        private void OnSequenced(SequencedEvent sequencedEvent)
        {
            long globalSeqNo = sequencedEvent.GlobalSeqNo;
            Assert.True(_expectedNext == globalSeqNo, $"{_tag}: dispatched {globalSeqNo}, expected {_expectedNext}");
            Assert.True(globalSeqNo <= _tip, _tag + ": dispatched a frame that was never published");
            ++_expectedNext;
            if (!_frames.ContainsKey(globalSeqNo))
            {
                ++_heartbeats;
                _heartbeatSum += globalSeqNo;
            }
        }

        // ── ISnapshotRestoreHandler: the restored state ────────────────────────────────────────────────

        public bool SupportsFormatVersion(long formatVersion)
        {
            return formatVersion == FormatVersion;
        }

        public void OnSnapshotHeader(SnapshotHeader header)
        {
            Assert.True(_expectedNext == Cut + 1, _tag + ": a restore after a frame was dispatched");
            _heartbeats = 0;
            _heartbeatSum = 0;
            _nextRecordIndex = 0;
        }

        public void OnSnapshotRecord(IDirectBuffer record, int length, int recordIndex)
        {
            Assert.True(_nextRecordIndex++ == recordIndex, _tag + ": records out of order");
            if (recordIndex == 0)
            {
                _heartbeats = record.GetLong(0);
            }
            else
            {
                _heartbeatSum = record.GetLong(0);
            }
        }

        private bool Chance(int percent)
        {
            return _rng.Chance(percent);
        }

        // Where frame `globalSeqNo` starts in the active recording: frame 1 at its base.
        private long PositionOf(long globalSeqNo)
        {
            return _positionBase + (globalSeqNo - 1) * Stride;
        }
    }
}
