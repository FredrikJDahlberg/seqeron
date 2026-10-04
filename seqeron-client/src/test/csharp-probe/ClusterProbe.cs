using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Replayer.Client;
using Org.Limitless.Seqeron.Sequencer.Client;
using Org.Limitless.Seqeron.Util;

namespace Org.Limitless.Seqeron.Tools;

/// <summary>
/// The C# client tier against a live cluster: the twin of <c>tools/ClusterProbe</c>'s <c>confirm</c> and
/// <c>follow</c> modes, speaking the same <c>ProbeMarker</c> (payloadId 5, sourceId 8) and printing the same
/// verdict lines, so <c>csharp-client-test.sh</c> judges it as <c>failover-test.sh</c> judges the Java one.
/// <list type="bullet">
/// <item><c>confirm</c>: catches up on the co-located tap, then streams <c>--count</c> markers through an
/// <see cref="IngressPublisher"/> — tracked by <see cref="PendingSends"/> unless <c>--untracked</c> — and exits 0 iff
/// its own tap showed 1..count exactly once, in order. <c>--colocated</c> connects through the member's IPC ingress
/// first, falling back to UDP.</item>
/// <item><c>follow</c>: cold-starts a <see cref="ReplayerStreamReceiver"/>, follows the tap live for
/// <c>--live-seconds</c> after catching up, and exits 0 iff it dispatched every frame from <c>globalSeqNo</c> 1 with
/// no gap.</item>
/// </list>
/// <c>ProbeMarker</c> is encoded by hand: sbe-probe.xml (schema 214) generates no C# codecs.
/// </summary>
public static class ClusterProbe
{
    private const int ProbePayloadId = 5;
    private const int ProbeSourceId = 8;
    private const int NoConnection = -1;

    // ProbeMarker's MessageHeader, then seqNo uint64, then filler as uint16-length var data.
    private const int ProbeMarkerTemplateId = 1;
    private const int ProbeSchemaId = 214;
    private const int ProbeMarkerBlockLength = sizeof(long);
    private const int MessageHeaderLength = 8;
    private const int ProbeMarkerLength = MessageHeaderLength + ProbeMarkerBlockLength + sizeof(ushort);

    private const int PendingCapacity = 4096;
    private const long IpcConnectTimeoutMs = 2_000;
    private const long DrainTimeoutMs = 15_000;

    /// <summary>Runs one mode.</summary>
    /// <param name="args">the mode, then <c>--name value</c> options and flags</param>
    /// <returns>0 on a clean verdict</returns>
    public static int Main(string[] args)
    {
        if (args.Length == 0)
        {
            return Usage();
        }
        Options options = Options.Parse(args.Skip(1).ToArray());
        try
        {
            switch (args[0])
            {
                case "confirm":
                    return Confirm(options);
                case "follow":
                    return Follow(options);
                default:
                    return Usage();
            }
        }
        catch (Exception ex) when (ex is AeronException || ex is InvalidOperationException)
        {
            Logger.Error(Logger.CoreComponent.ClusterProbe, Logger.CoreEventCode.ClusterSessionError, options.MemberId,
                         "{0} failed: {1}", args[0], ex);
            return 1;
        }
    }

    private static int Confirm(Options options)
    {
        bool tracked = !options.Flag("untracked");
        int count = options.Int("count", 1000);
        long pacingMicros = options.Int("pacing-micros", 0);
        var pending = new PendingSends(PendingCapacity);
        var own = new OwnFrames();
        bool caughtUp = false;
        var idle = new YieldingIdleStrategy();

        using Aeron aeron = Connect(options);
        using var sender = new ClusterStreamSender();
        using var receiver = new ReplayerStreamReceiver(
            options.ClientId,
            sequencedEvent =>
            {
                if (tracked)
                {
                    pending.OnSequenced(sequencedEvent);
                }
                own.OnSequenced(sequencedEvent);
            },
            (leader, term, globalSeqNo) => pending.OnLeadershipChanged(term), () => caughtUp = true);
        receiver.Start(aeron, options.MemberId);
        while (!caughtUp)
        {
            idle.Idle(receiver.Poll());
        }
        if (tracked)
        {
            sender.SetIngressHold(pending);
        }
        if (options.Flag("colocated"))
        {
            sender.ConnectColocated(aeron, options.MemberId, IpcConnectTimeoutMs, EgressChannel(options),
                                    PortLayout.IngressEndpoints());
        }
        else
        {
            sender.Connect(aeron, EgressChannel(options), PortLayout.IngressEndpoints());
        }
        var publisher = new IngressPublisher(tracked ? pending : null);
        var marker = new ProbeMarker();
        Logger.Info(Logger.CoreComponent.ClusterProbe, options.MemberId, "confirm: sending {0} frame(s){1}", count,
                    tracked ? " through PendingSends" : " untracked (control)");

        long next = 1;
        long nextSendTicks = 0;
        long lastProgressMs = Clocks.MonotonicMs();
        int resent = 0;
        while (true)
        {
            long nowMs = Clocks.MonotonicMs();
            int seen = own.SeqNos.Count;
            int work = receiver.Poll() + sender.PollEgress();
            sender.KeepAlive();
            if (sender.IsSessionLost || pending.IsFaulted)
            {
                Logger.Error(Logger.CoreComponent.ClusterProbe, Logger.CoreEventCode.ClusterSessionError,
                             options.MemberId, "confirm: {0}",
                             pending.IsFaulted ? "PendingSends faulted" : "cluster session lost");
                return 1;
            }
            int resentNow = tracked ? pending.ResendMissing(sender) : 0;
            if (resentNow > 0)
            {
                resent += resentNow;
                own.Sessions.Add(sender.ClusterSessionId);
            }
            long nowTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            if (next <= count && nowTicks >= nextSendTicks)
            {
                Publish outcome = publisher.PublishPayload(sender, ProbeSourceId, NoConnection, ProbePayloadId,
                                                           marker.Encode(next), ProbeMarkerLength);
                if (outcome == Publish.Refused)
                {
                    throw new InvalidOperationException("a ProbeMarker the sequencer would reject");
                }
                if (outcome == Publish.Published)
                {
                    own.Sessions.Add(sender.ClusterSessionId);
                    next++;
                    nextSendTicks = nowTicks + System.Diagnostics.Stopwatch.Frequency * pacingMicros / 1_000_000;
                    lastProgressMs = nowMs;
                }
            }
            if (resentNow > 0 || own.SeqNos.Count > seen)
            {
                lastProgressMs = nowMs;
            }
            bool drained = next > count && (tracked ? pending.Count == 0 : own.Last == count);
            if (drained || nowMs - lastProgressMs > DrainTimeoutMs)
            {
                break;
            }
            idle.Idle(work + resentNow);
        }
        return own.Judge(options.MemberId, count, resent);
    }

    private static int Follow(Options options)
    {
        long liveMs = options.Int("live-seconds", 5) * 1000L;
        long first = 0;
        long last = 0;
        long gaps = 0;
        bool caughtUp = false;
        var idle = new YieldingIdleStrategy();

        using Aeron aeron = Connect(options);
        ReplayerStreamReceiver receiver = null;
        receiver = new ReplayerStreamReceiver(options.ClientId,
                                              sequencedEvent =>
                                              {
                                                  long globalSeqNo = sequencedEvent.GlobalSeqNo;
                                                  if (first == 0)
                                                  {
                                                      first = globalSeqNo;
                                                  }
                                                  else if (globalSeqNo != last + 1)
                                                  {
                                                      gaps++;
                                                  }
                                                  last = globalSeqNo;
                                              },
                                              null,
                                              () =>
                                              {
                                                  if (!caughtUp)
                                                  {
                                                      Logger.Info(Logger.CoreComponent.ClusterProbe, options.MemberId,
                                                                  "Caught up at globalSeqNo {0} — following live",
                                                                  receiver.LastGlobalSeqNo);
                                                  }
                                                  caughtUp = true;
                                              });
        using (receiver)
        {
            receiver.Start(aeron, options.MemberId);
            Logger.Info(Logger.CoreComponent.ClusterProbe, options.MemberId,
                        "Following the node tap — replaying history via the Replayer, then live");
            long deadlineMs = long.MaxValue;
            while (Clocks.MonotonicMs() < deadlineMs)
            {
                if (caughtUp && deadlineMs == long.MaxValue)
                {
                    deadlineMs = Clocks.MonotonicMs() + liveMs;
                }
                idle.Idle(receiver.Poll());
            }
        }
        bool exact = first == 1 && gaps == 0 && caughtUp;
        Logger.Info(Logger.CoreComponent.ClusterProbe, options.MemberId,
                    "follow: {0} — dispatched globalSeqNo {1} to {2}, gaps {3}",
                    exact ? "CONTIGUOUS" : "NOT CONTIGUOUS", first, last, gaps);
        return exact ? 0 : 1;
    }

    private static Aeron Connect(Options options)
    {
        string aeronDir =
            options.String("aeron-dir", Path.Combine(Path.GetTempPath(), "seqeron-seq-aeron-" + options.MemberId));
        return Aeron.Connect(new Aeron.Context().AeronDirectoryName(aeronDir));
    }

    // This client's own egress endpoint, on an ephemeral port of its member's host.
    private static string EgressChannel(Options options)
    {
        string host =
            options.MemberId < PortLayout.Hosts.Count ? PortLayout.Hosts[options.MemberId] : PortLayout.DefaultHost;
        return "aeron:udp?endpoint=" + host + ":0";
    }

    private static int Usage()
    {
        Console.Error.WriteLine(
            "usage: Seqeron.ClusterProbe confirm|follow --member <id> --client-id <id> [--aeron-dir <dir>]\n" +
            "         confirm: [--count <n>] [--pacing-micros <us>] [--untracked] [--colocated]\n" +
            "         follow:  [--live-seconds <s>]");
        return 1;
    }

    // One ProbeMarker with no filler, written in place.
    private sealed class ProbeMarker
    {
        private readonly UnsafeBuffer _payload = new UnsafeBuffer(new byte[ProbeMarkerLength]);

        public ProbeMarker()
        {
            _payload.PutShort(0, ProbeMarkerBlockLength);
            _payload.PutShort(2, ProbeMarkerTemplateId);
            _payload.PutShort(4, ProbeSchemaId);
            _payload.PutShort(6, 0);
            _payload.PutShort(MessageHeaderLength + ProbeMarkerBlockLength, 0);
        }

        public UnsafeBuffer Encode(long seqNo)
        {
            _payload.PutLong(MessageHeaderLength, seqNo);
            return _payload;
        }
    }

    // The seqNos this process's own sessions put on the tap, in tap order.
    private sealed class OwnFrames
    {
        public readonly HashSet<long> Sessions = new HashSet<long>();
        public readonly List<long> SeqNos = new List<long>();

        public long Last => SeqNos.Count == 0 ? 0 : SeqNos[SeqNos.Count - 1];

        public void OnSequenced(SequencedEvent sequencedEvent)
        {
            if (sequencedEvent.IsSystem || sequencedEvent.PayloadId != ProbePayloadId ||
                sequencedEvent.TemplateId != ProbeMarkerTemplateId ||
                !Sessions.Contains(sequencedEvent.SourceSessionId))
            {
                return;
            }
            SeqNos.Add(sequencedEvent.Buffer.GetLong(sequencedEvent.PayloadOffset + MessageHeaderLength,
                                                     ByteOrder.LittleEndian));
        }

        // 0 if the tap showed 1..count exactly once, in order; logs the tally either way.
        public int Judge(int memberId, int count, int resent)
        {
            var distinct = new HashSet<long>(SeqNos);
            int outOfOrder = 0;
            for (int i = 1; i < SeqNos.Count; i++)
            {
                if (SeqNos[i] <= SeqNos[i - 1])
                {
                    outOfOrder++;
                }
            }
            int missing = count - distinct.Count;
            int duplicated = SeqNos.Count - distinct.Count;
            bool exact = missing == 0 && duplicated == 0 && outOfOrder == 0;
            Logger.Info(
                Logger.CoreComponent.ClusterProbe, memberId,
                "confirm: {0} — tap showed {1} of {2}: missing {3}, duplicated {4}, out of order {5}; resent {6}",
                exact ? "EXACT" : "NOT EXACT", SeqNos.Count, count, missing, duplicated, outOfOrder, resent);
            return exact ? 0 : 1;
        }
    }

    private sealed class Options
    {
        private readonly Dictionary<string, string> _values = new Dictionary<string, string>();
        private readonly HashSet<string> _flags = new HashSet<string>();

        public int MemberId => Int("member", 0);

        public int ClientId => Int("client-id", 9);

        public static Options Parse(string[] args)
        {
            var options = new Options();
            for (int i = 0; i < args.Length; i++)
            {
                string name = args[i].TrimStart('-');
                if (i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal))
                {
                    options._values[name] = args[++i];
                }
                else
                {
                    options._flags.Add(name);
                }
            }
            return options;
        }

        public bool Flag(string name)
        {
            return _flags.Contains(name);
        }

        public int Int(string name, int fallback)
        {
            return _values.TryGetValue(name, out string value) ? int.Parse(value) : fallback;
        }

        public string String(string name, string fallback)
        {
            return _values.GetValueOrDefault(name, fallback);
        }
    }
}
