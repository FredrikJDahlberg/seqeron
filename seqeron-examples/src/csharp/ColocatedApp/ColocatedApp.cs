using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.App;
using Publish = Org.Limitless.Seqeron.Protocol.Publish;

namespace Example;

/// <summary>
/// The same flow as FollowStream, written against the front door instead of the tiers under it. Look at the
/// usings: the App namespace, the Publish alias, Aeron and Agrona, and nothing else. There is no receiver here, no
/// sender, no envelope and no <c>systemEventType</c> — the façade assembles the cluster session, the tap, confirmed
/// ingress across a failover, the fences and the leader gate, and hands this class <see cref="Payload"/>s.
/// <para>A co-located application is the producer kind nothing elects: one replica per node, publishing only while
/// its own node leads. That is why it needs no topology document to run — <c>LeadershipChanged</c> already picks
/// the replica that submits, so <see cref="OnLeadershipChanged"/> is the whole election. The C# twin of
/// <c>ColocatedApp.java</c>.</para>
/// <para>Environment: <c>SEQERON_NODE_MEMBER_ID</c> (default 0), <c>SEQERON_REPLAYER_CLIENT_ID</c> (default 20),
/// <c>SEQERON_AERON_DIR</c>.</para>
/// </summary>
public sealed class ColocatedApp : IApplicationListener
{
    // The examples' payloadId — eight raw bytes, no schema, which spec §13.2 admits.
    private const int PingPayloadId = 6;

    // This example's own sourceId (spec §5); Java's is 11 and C++'s 12.
    private const int SourceId = 17;

    // Ephemeral: one session, and no port of its own to allocate.
    private const string EgressChannel = "aeron:udp?endpoint=localhost:0";

    // Agrona.NET's backoff parks for up to 16 ms by default; Java's for at most 1 ms, which this keeps.
    private const long MaxParkPeriodMs = 1;

    private static readonly long PingIntervalTicks = Stopwatch.Frequency;

    private Application _app;
    private volatile bool _running = true;
    private long _pingSentTicks;
    private string _fence;

    public static int Main()
    {
        int memberId = EnvInt("SEQERON_NODE_MEMBER_ID", 0);
        int clientId = EnvInt("SEQERON_REPLAYER_CLIENT_ID", 20);
        string aeronDir = Environment.GetEnvironmentVariable("SEQERON_AERON_DIR") ??
                          Path.Combine(Path.GetTempPath(), "seqeron-seq-aeron-" + memberId);

        var application = new ColocatedApp();
        using PosixSignalRegistration sigint = PosixSignalRegistration.Create(PosixSignal.SIGINT, application.Stop);
        using PosixSignalRegistration sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, application.Stop);

        // IngressEndpoints is not set: it defaults to the cluster's own port block, which is what a co-located
        // replica falls back to on the duty cycles where its node is not the one leading.
        using Aeron aeron = Aeron.Connect(new Aeron.Context().AeronDirectoryName(aeronDir));
        using var app =
            new Application(new ApplicationOptions { SourceId = SourceId, ClientId = clientId, MemberId = memberId,
                                                     EgressChannel = EgressChannel, Listener = application });
        application._app = app;
        app.Start(aeron);
        Console.WriteLine($"# member {memberId} via {aeronDir} — replica of application sourceId {SourceId}");
        application.Run();
        if (application._fence != null)
        {
            Console.Error.WriteLine("# " + application._fence);
            return 1;
        }
        return 0;
    }

    // The one duty cycle. Every member of the façade belongs to this thread, callbacks included.
    private void Run()
    {
        var idle = new BackoffIdleStrategy(Configuration.IDLE_MAX_SPINS, Configuration.IDLE_MAX_YIELDS,
                                           Configuration.IDLE_MIN_PARK_MS, MaxParkPeriodMs);
        long nextPingTicks = 0;
        while (_running && _fence == null)
        {
            int work = _app.DoWork();
            long now = Stopwatch.GetTimestamp();
            // CanPublish is the gate and the failover hold in one: shut while this node does not lead, and shut while
            // PendingSends is still resending what the last leader change lost.
            if (_app.CanPublish && now >= nextPingTicks)
            {
                Ping();
                nextPingTicks = now + PingIntervalTicks;
            }
            idle.Idle(work);
        }
    }

    private void Stop(PosixSignalContext context)
    {
        context.Cancel = true;
        _running = false;
    }

    // One payload at cluster ingress, on this application's own sourceId and no connection, from a span: the façade
    // copies it once into a buffer of its own. Nothing waits here for the echo: this process is its own consumer and
    // picks it up off the tap like any other frame.
    private void Ping()
    {
        _pingSentTicks = Stopwatch.GetTimestamp();
        Span<byte> body = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(body, _pingSentTicks);
        if (_app.Publish(PingPayloadId, body) != Publish.Published)
        {
            // Declined: the gate shut, ingress is held behind a resend, or the transport is back-pressured. Next
            // second's ping is the retry.
            _pingSentTicks = 0;
        }
    }

    // Every application payload on this node's tap, in globalSeqNo order, history and live alike — every replica sees
    // the same ones and so holds the same state. System frames never arrive here; the façade acts on them and reports
    // only what an application has a decision to make about.
    public void OnSequenced(Payload payload)
    {
        if (IsOwnPing(payload))
        {
            long roundTripUs = (long)Stopwatch.GetElapsedTime(_pingSentTicks).TotalMicroseconds;
            Console.WriteLine($"{payload.GlobalSeqNo} ping echoed, round trip {roundTripUs}us");
        }
        else
        {
            Console.WriteLine($"{payload.GlobalSeqNo} sourceId={payload.SourceId} payloadId={payload.PayloadId} " +
                              $"length={payload.PayloadLength}");
        }
    }

    // This process's own ping, told from the other examples' by the timestamp it carries.
    private bool IsOwnPing(Payload payload)
    {
        return _pingSentTicks != 0 && payload.SourceId == SourceId && payload.PayloadId == PingPayloadId &&
               payload.PayloadLength == sizeof(long) &&
               payload.Buffer.GetLong(payload.PayloadOffset, ByteOrder.LittleEndian) == _pingSentTicks;
    }

    // The whole election, for the producer kind nothing elects. False is also where a replica keeping outstanding
    // work calls OutstandingWork.OnNotLeader() — every leadership change shuts an open gate, and a payload submitted
    // during the election may have gone with it.
    public void OnLeadershipChanged(bool leading)
    {
        Console.WriteLine(leading ? "# leading — publishing" : "# not leading — silent");
    }

    public void OnCaughtUp(long globalSeqNo)
    {
        Console.WriteLine($"# caught up at {globalSeqNo} — following the tap live");
    }

    // The cluster clock, once a second: the one time source that keeps advancing while every producer is silent, and
    // identical on every node. A deadline belongs on this rather than on a local clock.
    public void OnClusterHeartbeat(long clusterTimeNs, long receiveTimeNs)
    {
        // Nothing to time here; a producer with a watchdog runs it off this.
    }

    // Latched, once: this replica may no longer act, and exiting lets its restart re-walk the log. Recorded rather
    // than thrown — this runs inside the façade's duty cycle, which raises nothing of a consumer's on its behalf.
    public void OnFenced(ClusterError fence, string detail)
    {
        _fence = fence + ": " + detail;
    }

    private static int EnvInt(string name, int fallback)
    {
        string value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrEmpty(value) ? fallback : int.Parse(value);
    }
}
