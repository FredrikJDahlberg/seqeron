using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Text;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.App;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Util;

namespace Org.Limitless.Seqeron.Tools;

/// <summary>
/// The hop through the C# <see cref="Application"/> on the leader's driver (doc/ec2-tests.md, Test 1b): an open-loop
/// publish of a ProbeMarker of <c>--payload</c> bytes, timed from its intended send until this process sees its own
/// frame sequenced. It prints the line <c>hosted_hop_bench</c> does, so <c>hop-knee.sh</c> reads either.
/// <code>
///   Seqeron.HopBench --rate R/s --count N [--member M] [--aeron-dir D] [--payload B] [--warmup-ms W] [--source-id S]
///                    [--client-id C]
/// </code>
/// The first <c>--warmup-ms</c> of the run (default a fifth of it, at least 2000 markers) is discarded, and so are the
/// declines before it. The idle strategy is <c>SEQERON_IDLE_STRATEGY</c>.
/// </summary>
public static class HopBench
{
    private const int ProbePayloadId = 5;
    private const int ProbeMarkerTemplateId = 1;
    private const int ProbeSchemaId = 214;
    private const int MarkerBlockLength = 8;
    private const int Header = 8;
    private const int MinPayload = Header + MarkerBlockLength + sizeof(ushort);
    private const int MaxPayload = 8875;

    // Set in the seqNo of every marker to tell them from other sources'; bits 28-39 salt it per run.
    private const long OwnTag = 1L << 40;
    private const int SaltShift = 28;
    private const long IndexMask = (1L << SaltShift) - 1;
    private const long TimeoutNs = 90_000_000_000L;
    private const long StartDelayNs = 200_000_000L;

    public static int Main(string[] args)
    {
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
        var options = new Dictionary<string, string>();
        for (int i = 0; i + 1 < args.Length; i += 2)
        {
            options[args[i].TrimStart('-')] = args[i + 1];
        }
        try
        {
            return Execute(options);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"HopBench failed: {ex}");
            return 1;
        }
    }

    private static long NowNs()
    {
        return (long)(Stopwatch.GetTimestamp() * (1_000_000_000.0 / Stopwatch.Frequency));
    }

    private static int Execute(Dictionary<string, string> options)
    {
        string Get(string name, string fallback) => options.TryGetValue(name, out string value) ? value : fallback;

        long rate = long.Parse(Get("rate", "5000"));
        int count = int.Parse(Get("count", "30000"));
        int member = int.Parse(Get("member", "0"));
        int payload = int.Parse(Get("payload", MinPayload.ToString()));
        int sourceId = int.Parse(Get("source-id", "16"));
        if (rate <= 0 || count <= 0 || count > IndexMask || payload < MinPayload || payload > MaxPayload)
        {
            throw new ArgumentException("rate and count must be positive and payload within 18..8875");
        }

        var run = new Run(rate, count);
        long warmupMs = long.Parse(Get("warmup-ms", "-1"));
        int warm =
            (int)Math.Min(count, warmupMs >= 0 ? warmupMs * 1_000_000L / run.Interval : Math.Max(2000, count / 5));
        var listener = new Listener(run, sourceId);
        var marker = new UnsafeBuffer(new byte[payload]);
        marker.PutShort(0, MarkerBlockLength);
        marker.PutShort(2, ProbeMarkerTemplateId);
        marker.PutShort(4, ProbeSchemaId);
        IIdleStrategy idle = IdleStrategies.FromEnvironment()();
        long declinesAtWarm = 0;

        using (
            var aeron = Aeron.Connect(new Aeron.Context().AeronDirectoryName(Get(
                "aeron-dir", PortLayout.AeronDir(member))))) using (var app = new Application(new ApplicationOptions {
                                                                        SourceId = sourceId,
                                                                        ClientId = int.Parse(Get("client-id", "12")),
                                                                        MemberId = member,
                                                                        Listener = listener,
                                                                    }))
        {
            app.Start(aeron);
            while (!run.Done(NowNs()) && run.Fault == null)
            {
                int work = app.DoWork();
                long now = NowNs();
                if (run.T0 == 0)
                {
                    if (app.CanPublish)
                    {
                        run.T0 = now + StartDelayNs;
                        run.LastNs = run.T0;
                    }
                }
                else if (run.Sent < count && now >= run.Due(run.Sent) && app.CanPublish)
                {
                    marker.PutLong(Header, OwnTag | run.Salt | (run.Sent + 1));
                    Publish outcome = app.Publish(ProbePayloadId, marker, payload);
                    if (outcome == Publish.Published)
                    {
                        run.Sent++;
                        work++;
                        if (run.Sent == warm)
                        {
                            declinesAtWarm = run.Declines;
                        }
                    }
                    else if (outcome == Publish.Refused)
                    {
                        throw new InvalidOperationException("a ProbeMarker the sequencer would refuse");
                    }
                    else
                    {
                        run.Declines++;
                    }
                }
                idle.Idle(work);
            }
        }
        if (run.Fault != null)
        {
            Console.WriteLine($"FENCED {run.Fault}");
            return 1;
        }
        Report(run, rate, payload, warm, declinesAtWarm);
        return 0;
    }

    // One run's open-loop schedule and the latencies it produced; marker i is sent at T0 + i * interval.
    private sealed class Run
    {
        public Run(long rate, long count)
        {
            Count = count;
            Interval = 1_000_000_000L / rate;
            Salt = ((NowNs() / 1_000_000_000L) & 0xFFF) << SaltShift;
            Latency = new long[count];
            Array.Fill(Latency, -1L);
        }

        public long Count { get; }
        public long Interval { get; }
        public long Salt { get; }
        public long[] Latency { get; }
        public long T0 { get; set; }
        public long Sent { get; set; }
        public long Seen { get; private set; }
        public long Declines { get; set; }
        public long LastNs { get; set; }
        public string Fault { get; set; }

        public long Due(long index) => T0 + index * Interval;

        public void Received(long seqNo, long now)
        {
            long index = (seqNo & IndexMask) - 1;
            if ((seqNo & (OwnTag - 1) & ~IndexMask) != Salt || index < 0 || index >= Sent || Latency[index] >= 0)
            {
                return;
            }
            Latency[index] = now - Due(index);
            Seen++;
            LastNs = now;
        }

        public bool Done(long now) => Seen >= Count || (T0 != 0 && now > T0 + TimeoutNs + Count * Interval);
    }

    private sealed class Listener : IApplicationListener
    {
        private readonly Run _run;
        private readonly int _sourceId;

        public Listener(Run run, int sourceId)
        {
            _run = run;
            _sourceId = sourceId;
        }

        public void OnLeadershipChanged(bool leading)
        {
        }

        public void OnSequenced(Payload payload)
        {
            if (payload.SourceId == _sourceId && payload.PayloadId == ProbePayloadId &&
                payload.TemplateId == ProbeMarkerTemplateId)
            {
                long seqNo = payload.Buffer.GetLong(payload.BodyOffset, ByteOrder.LittleEndian);
                if ((seqNo & OwnTag) != 0)
                {
                    _run.Received(seqNo, NowNs());
                }
            }
        }

        public void OnFenced(ClusterError fence, string detail)
        {
            _run.Fault = $"{fence}: {detail}";
        }
    }

    // The samples as bucket:count pairs, as hosted_hop_bench writes them: 32 buckets to each power of two.
    private static string Histogram(long[] samples)
    {
        var buckets = new SortedDictionary<int, long>();
        foreach (long value in samples)
        {
            int log = value < 32 ? 0 : 63 - BitOperations.LeadingZeroCount((ulong)value);
            int bucket = value < 32 ? (int)value : (log - 4) * 32 + (int)((value >> (log - 5)) & 31);
            buckets[bucket] = buckets.GetValueOrDefault(bucket) + 1;
        }
        var text = new StringBuilder();
        foreach (var pair in buckets)
        {
            text.Append(text.Length == 0 ? "" : ",").Append(pair.Key).Append(':').Append(pair.Value);
        }
        return text.ToString();
    }

    private static void Report(Run run, long rate, int payload, int warm, long declinesAtWarm)
    {
        var list = new List<long>();
        for (int i = warm; i < run.Latency.Length; i++)
        {
            if (run.Latency[i] >= 0)
            {
                list.Add(run.Latency[i]);
            }
        }
        long[] samples = list.ToArray();
        Array.Sort(samples);
        if (samples.Length == 0)
        {
            Console.WriteLine($"arm=csharp rate={rate}: no samples (seen {run.Seen} of {run.Count})");
            return;
        }
        double At(double quantile) => samples[(int)(quantile * (samples.Length - 1))] / 1000.0;
        Console.WriteLine(
            $"arm=csharp rate={rate} payload={payload} n={samples.Length} seen={run.Seen} " +
            $"declines={run.Declines - declinesAtWarm} warmup_declines={declinesAtWarm} " +
            $"p50={At(0.5):F1} p99={At(0.99):F1} p99.9={At(0.999):F1} max={At(1.0):F1} (us) hist={Histogram(samples)}");
    }
}
