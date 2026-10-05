using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Replayer.Client;
using Org.Limitless.Seqeron.Sequencer.Client;
using Org.Limitless.Seqeron.Util;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;
using SbeBuffer = Org.SbeTool.Sbe.Dll.DirectBuffer;

namespace Example;

/// <summary>
/// Follows one node's ordered stream end to end: history replayed through that node's co-located ReplayerService,
/// then the live tap, with the switch between them handled by the receiver. Once caught up it also produces — one
/// ping a second at cluster ingress, whose echo comes back through <see cref="OnSequenced"/> with everything else.
/// <para>Both families are exercised in both directions: the ping is an application payload, and the connection
/// this example announces is a system event, submitted with <c>PublishSystem</c> and decoded off the tap in
/// <see cref="PrintSystem"/>. The C# twin of <c>FollowStream.java</c>.</para>
/// <para>Environment: <c>SEQERON_NODE_MEMBER_ID</c> (default 0), <c>SEQERON_REPLAYER_CLIENT_ID</c> (default 19),
/// <c>SEQERON_AERON_DIR</c>, <c>SEQERON_IDLE_STRATEGY</c>.</para>
/// </summary>
public sealed class FollowStream
{
    // The examples' own payloadId and sourceId.
    private const int PingPayloadId = 6;
    private const int PingSourceId = 10;

    // The one connection this example models: announced at start-up, and what every ping rides.
    private const int ConnectionId = 1;

    // Ingress is tried on this member's own aeron:ipc first; a follower answers there on neither.
    private const long IpcConnectTimeoutMs = 500;

    // Ephemeral: one session, and no port of its own to allocate.
    private const string EgressChannel = "aeron:udp?endpoint=localhost:0";

    // Whatever identity a producer's connections have; opaque to the cluster tier, and MAY be empty.
    private static readonly byte[] ConnectionLabel = Encoding.ASCII.GetBytes("follow-example");

    private static readonly long PingIntervalTicks = Stopwatch.Frequency;

    private readonly IngressPublisher _publisher = new IngressPublisher();
    private readonly UnsafeBuffer _pingBody = new UnsafeBuffer(GC.AllocateArray<byte>(sizeof(long), true));
    private readonly UnsafeBuffer _systemBody = new UnsafeBuffer(GC.AllocateArray<byte>(64, true));
    private readonly SbeBuffer _view = new SbeBuffer();
    private readonly Frame.ConnectionOpened _connectionOpened = new Frame.ConnectionOpened();
    private readonly Frame.ConnectionClosed _connectionClosed = new Frame.ConnectionClosed();
    private readonly Frame.ClusterHeartbeat _heartbeat = new Frame.ClusterHeartbeat();
    private readonly Frame.GatewayActive _gatewayActive = new Frame.GatewayActive();

    private volatile bool _running = true;
    private long _lastGlobalSeqNo;
    private string _fault;
    private long _pingSentTicks;

    public static int Main()
    {
        int memberId = EnvInt("SEQERON_NODE_MEMBER_ID", 0);
        int clientId = EnvInt("SEQERON_REPLAYER_CLIENT_ID", 19);
        string aeronDir = Environment.GetEnvironmentVariable("SEQERON_AERON_DIR") ??
                          Path.Combine(Path.GetTempPath(), "seqeron-seq-aeron-" + memberId);
        return new FollowStream().Run(memberId, clientId, aeronDir);
    }

    private int Run(int memberId, int clientId, string aeronDir)
    {
        using PosixSignalRegistration sigint = PosixSignalRegistration.Create(PosixSignal.SIGINT, Stop);
        using PosixSignalRegistration sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, Stop);

        // ConnectColocated is what a co-located producer wants: ingress over its own member's aeron:ipc — no
        // endpoints, no ports — falling back to the UDP endpoint set when that member is not the leader, which is
        // the only member that subscribes to IPC ingress.
        using Aeron aeron = Aeron.Connect(new Aeron.Context().AeronDirectoryName(aeronDir));
        using var sender = new ClusterStreamSender();
        using var receiver =
            new ReplayerStreamReceiver(clientId, OnSequenced, OnLeadershipChanged,
                                       () => Console.WriteLine("# caught up — following the tap live"));
        sender.ConnectColocated(aeron, memberId, IpcConnectTimeoutMs, EgressChannel, PortLayout.IngressEndpoints());
        receiver.Start(aeron, memberId);
        Console.WriteLine($"# following member {memberId} via {aeronDir}");

        // The one duty cycle. Every receiver and sender method belongs to this thread.
        IIdleStrategy idle = IdleStrategies.FromEnvironment()();
        bool announced = false;
        long nextPingTicks = 0;
        while (_running && _fault == null)
        {
            int work = receiver.Poll() + sender.PollEgress();
            // Self-throttling: the sender decides when a keep-alive is due, so this just says when it had the
            // chance to send one.
            sender.KeepAlive();
            if (!announced)
            {
                announced = AnnounceConnection(sender);
            }
            // Only once caught up: a ping submitted during the replay walk would be echoed behind the history
            // still being read, and the round trip would measure the walk rather than the path.
            long now = Stopwatch.GetTimestamp();
            if (announced && receiver.IsCaughtUp && now >= nextPingTicks)
            {
                Ping(sender);
                nextPingTicks = now + PingIntervalTicks;
            }
            idle.Idle(work);
        }
        if (announced)
        {
            CloseConnection(sender);
        }
        if (_fault != null)
        {
            Console.Error.WriteLine("# " + _fault);
            return 1;
        }
        return 0;
    }

    private void Stop(PosixSignalContext context)
    {
        context.Cancel = true;
        _running = false;
    }

    // This example's one connection, as a ConnectionOpened system event: a system payload goes through
    // PublishSystem, and carries no MessageHeader because systemEventType names it. A Declined is retried on the
    // next duty cycle.
    private bool AnnounceConnection(ClusterStreamSender sender)
    {
        SbeBuffers.Wrap(_view, _systemBody, 0, _systemBody.Capacity);
        _connectionOpened.WrapForEncode(_view, 0);
        _connectionOpened.SetConnectionData(ConnectionLabel);
        return _publisher.PublishSystem(sender, PingSourceId, ConnectionId, SystemFrame.ConnectionOpened, _systemBody,
                                        _connectionOpened.Size) == Publish.Published;
    }

    // The matching ConnectionClosed, which has no fields: header.connectionId names a connection every consumer
    // already saw open. Best effort — with no session left to take it, the connection stays open in the sequencer's
    // set, exactly as it would had this process crashed.
    private void CloseConnection(ClusterStreamSender sender)
    {
        SbeBuffers.Wrap(_view, _systemBody, 0, _systemBody.Capacity);
        _connectionClosed.WrapForEncode(_view, 0);
        _publisher.PublishSystem(sender, PingSourceId, ConnectionId, SystemFrame.ConnectionClosed, _systemBody,
                                 _connectionClosed.Size);
    }

    // One ping at cluster ingress: the examples' own payloadId, and a payload of eight raw bytes holding the
    // timestamp it left on. Not SBE, and it need not be — the cluster tier decodes no payloadId at all. Nothing waits
    // here for the echo: the consumer this process already is picks it up off the tap like every other frame.
    private void Ping(ClusterStreamSender sender)
    {
        _pingSentTicks = Stopwatch.GetTimestamp();
        _pingBody.PutLong(0, _pingSentTicks, ByteOrder.LittleEndian);
        if (_publisher.PublishPayload(sender, PingSourceId, ConnectionId, PingPayloadId, _pingBody, sizeof(long)) !=
            Publish.Published)
        {
            // Declined: the sender spun through back-pressure and an election and found no session at the end of
            // it. Next second's ping is the retry.
            _pingSentTicks = 0;
        }
    }

    // Every other frame arrives here in globalSeqNo order, history and live alike — the receiver requests a replay
    // for anything the live tap dropped and dispatches nothing out of order in the meantime.
    private void OnSequenced(SequencedEvent sequencedEvent)
    {
        if (!InOrder(sequencedEvent.GlobalSeqNo))
        {
            return;
        }
        if (sequencedEvent.IsSystem)
        {
            PrintSystem(sequencedEvent);
        }
        else if (IsOwnPing(sequencedEvent))
        {
            long roundTripUs = (long)Stopwatch.GetElapsedTime(_pingSentTicks).TotalMicroseconds;
            Console.WriteLine($"{sequencedEvent.GlobalSeqNo} ping echoed, round trip {roundTripUs}us");
        }
        else
        {
            Console.WriteLine($"{sequencedEvent.GlobalSeqNo} payloadId={sequencedEvent.PayloadId} " +
                              $"template={sequencedEvent.TemplateId} length={sequencedEvent.PayloadLength}");
        }
    }

    // A system payload decoded. The payload carries no MessageHeader, so the decoder supplies what one would have
    // said: its own BlockLength and SchemaVersion, for every system payload. Every allocated event arrives whether a
    // consumer handles it or not, so the default arm is where a consumer of one protocol spends its time.
    private void PrintSystem(SequencedEvent sequencedEvent)
    {
        long globalSeqNo = sequencedEvent.GlobalSeqNo;
        SbeBuffers.Wrap(_view, sequencedEvent.Buffer, sequencedEvent.PayloadOffset, sequencedEvent.PayloadLength);
        switch (sequencedEvent.SystemEventType)
        {
            case SystemFrame.ConnectionOpened:
                _connectionOpened.WrapForDecode(_view, 0, Frame.ConnectionOpened.BlockLength,
                                                Frame.ConnectionOpened.SchemaVersion);
                Console.WriteLine($"{globalSeqNo} ConnectionOpened connection={sequencedEvent.ConnectionId} " +
                                  $"label={_connectionOpened.ConnectionDataLength()} bytes");
                break;
            case SystemFrame.ClusterHeartbeat:
                _heartbeat.WrapForDecode(_view, 0, Frame.ClusterHeartbeat.BlockLength,
                                         Frame.ClusterHeartbeat.SchemaVersion);
                Console.WriteLine($"{globalSeqNo} ClusterHeartbeat cluster clock {_heartbeat.Header.Timestamp}ns");
                break;
            case SystemFrame.GatewayActive:
                // Only after clusterctl load-topology: this frame is the cluster designating one gateway instance, and
                // its gatewayId is in the payload and nowhere else.
                _gatewayActive.WrapForDecode(_view, 0, Frame.GatewayActive.BlockLength,
                                             Frame.GatewayActive.SchemaVersion);
                Console.WriteLine($"{globalSeqNo} GatewayActive gatewayId={_gatewayActive.GatewayId}");
                break;
            default:
                Console.WriteLine($"{globalSeqNo} system eventType={sequencedEvent.SystemEventType}");
                break;
        }
    }

    // This process's own ping, told from any other producer's by the timestamp it carries.
    private bool IsOwnPing(SequencedEvent sequencedEvent)
    {
        return _pingSentTicks != 0 && sequencedEvent.PayloadId == PingPayloadId &&
               sequencedEvent.PayloadLength == sizeof(long) &&
               sequencedEvent.Buffer.GetLong(sequencedEvent.PayloadOffset, ByteOrder.LittleEndian) == _pingSentTicks;
    }

    // The one frame family that reaches a consumer here instead of through OnSequenced.
    private void OnLeadershipChanged(int newLeaderMemberId, long leadershipTermId, long globalSeqNo)
    {
        if (!InOrder(globalSeqNo))
        {
            return;
        }
        Console.WriteLine($"{globalSeqNo} leader=member {newLeaderMemberId} term {leadershipTermId}");
    }

    // The invariant the whole tier exists for: one frame per globalSeqNo, no holes, replay and live alike. Recorded
    // rather than thrown — this runs inside a fragment handler, and Image.Poll advances the subscriber position
    // regardless of what a handler raises, so the duty cycle above raises it instead.
    private bool InOrder(long globalSeqNo)
    {
        if (_fault != null)
        {
            return false;
        }
        if (globalSeqNo != _lastGlobalSeqNo + 1)
        {
            _fault = "gap: " + _lastGlobalSeqNo + " -> " + globalSeqNo;
            return false;
        }
        _lastGlobalSeqNo = globalSeqNo;
        return true;
    }

    private static int EnvInt(string name, int fallback)
    {
        string value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrEmpty(value) ? fallback : int.Parse(value);
    }
}
