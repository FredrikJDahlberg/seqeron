using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.App;
using Publish = Org.Limitless.Seqeron.Protocol.Publish;

namespace Example;

/// <summary>
/// One instance of an elected active/standby pair, written against <see cref="Gateway"/> alone — the gateway twin
/// of ColocatedApp, and the C# twin of <c>GatewayApp.cpp</c>, under the same usings check.
/// <para>A gateway is the producer kind the cluster elects: the topology names both instances, the cluster
/// designates one, and only that one serves. What is left here is the edge. A real gateway opens a socket in
/// <see cref="OnActivated"/>; this one takes a single simulated client connection instead, and pings the cluster on
/// it once a second, reading each ping back off its own tap.</para>
/// <para>Load <c>seqeron-examples/topology-csharp.xml</c>, then run one instance or both; stop the first and the
/// second takes over. Environment: <c>SEQERON_EXAMPLE_GATEWAY_NAME</c> (default <c>GW-EX-CS-A</c>),
/// <c>SEQERON_NODE_MEMBER_ID</c> (default 0), <c>SEQERON_REPLAYER_CLIENT_ID</c> (default 21 for A, 22 for B),
/// <c>SEQERON_AERON_DIR</c>.</para>
/// </summary>
public sealed class GatewayApp : IGatewayListener
{
    // The examples' payloadId — eight raw bytes, no schema, which spec §13.2 admits.
    private const int PingPayloadId = 6;

    // Ephemeral: one session, and no port of its own to allocate.
    private const string EgressChannel = "aeron:udp?endpoint=localhost:0";

    // Agrona.NET's backoff parks for up to 16 ms by default; Java's for at most 1 ms, which this keeps.
    private const long MaxParkPeriodMs = 1;

    private static readonly byte[] ClientLabel = Encoding.ASCII.GetBytes("example-client");

    private static readonly long PingIntervalTicks = Stopwatch.Frequency;

    private volatile bool _running = true;
    private string _fault;
    private bool _open;
    private int _connectionId = Gateway.NoConnection;
    private long _pingSentTicks;

    public static int Main()
    {
        string gatewayName = Environment.GetEnvironmentVariable("SEQERON_EXAMPLE_GATEWAY_NAME");
        gatewayName = string.IsNullOrEmpty(gatewayName) ? "GW-EX-CS-A" : gatewayName;
        int instance = gatewayName == "GW-EX-CS-B" ? 1 : 0;
        int memberId = EnvInt("SEQERON_NODE_MEMBER_ID", 0);
        int clientId = EnvInt("SEQERON_REPLAYER_CLIENT_ID", 21 + instance);
        string aeronDir = Environment.GetEnvironmentVariable("SEQERON_AERON_DIR") ??
                          Path.Combine(Path.GetTempPath(), "seqeron-seq-aeron-" + memberId);

        var edge = new GatewayApp();
        using PosixSignalRegistration sigint = PosixSignalRegistration.Create(PosixSignal.SIGINT, edge.Stop);
        using PosixSignalRegistration sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, edge.Stop);

        using Aeron aeron = Aeron.Connect(new Aeron.Context().AeronDirectoryName(aeronDir));
        using var gateway =
            new Gateway(new GatewayOptions { GatewayName = gatewayName, ClientId = clientId, MemberId = memberId,
                                             EgressChannel = EgressChannel, Listener = edge });
        gateway.Start(aeron);
        Console.WriteLine($"# {gatewayName} on member {memberId} via {aeronDir}");
        edge.Run(gateway);
        if (edge._fault != null)
        {
            Console.Error.WriteLine("# " + edge._fault);
            return 1;
        }
        return 0;
    }

    // The one duty cycle. Every member of the façade belongs to this thread, callbacks included.
    private void Run(Gateway gateway)
    {
        var idle = new BackoffIdleStrategy(Configuration.IDLE_MAX_SPINS, Configuration.IDLE_MAX_YIELDS,
                                           Configuration.IDLE_MIN_PARK_MS, MaxParkPeriodMs);
        long nextPingTicks = 0;
        Span<byte> body = stackalloc byte[sizeof(long)];
        while (_running && _fault == null)
        {
            int work = gateway.DoWork();
            // CanAccept is serving and not held behind a failover's resend: the moment a real gateway accepts.
            if (_open && _connectionId == Gateway.NoConnection && gateway.CanAccept)
            {
                _connectionId = gateway.OpenConnection(ClientLabel);
            }
            long now = Stopwatch.GetTimestamp();
            if (_connectionId != Gateway.NoConnection && now >= nextPingTicks)
            {
                // Declined until the connection's ConnectionOpened has landed, and while ingress is held or
                // back-pressured. Next second's ping is the retry.
                _pingSentTicks = now;
                BinaryPrimitives.WriteInt64LittleEndian(body, now);
                if (gateway.Publish(_connectionId, PingPayloadId, body) != Publish.Published)
                {
                    _pingSentTicks = 0;
                }
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

    // Designated: open the edge. A real gateway binds its listen socket here; false would be retried.
    public bool OnActivated(int firstConnectionId)
    {
        Console.WriteLine($"# designated — serving, connection ids from {firstConnectionId}");
        _open = true;
        return true;
    }

    // Stood down or fenced: close the edge and drop every connection it let in.
    public void OnStandby()
    {
        Console.WriteLine("# standing by");
        _open = false;
        _connectionId = Gateway.NoConnection;
        _pingSentTicks = 0;
    }

    // Every application payload on this node's tap, in globalSeqNo order, history and live alike.
    public void OnSequenced(Payload payload)
    {
        if (_pingSentTicks != 0 && payload.ConnectionId == _connectionId && payload.PayloadId == PingPayloadId &&
            payload.PayloadLength == sizeof(long) &&
            payload.Buffer.GetLong(payload.PayloadOffset, ByteOrder.LittleEndian) == _pingSentTicks)
        {
            long roundTripUs = (long)Stopwatch.GetElapsedTime(_pingSentTicks).TotalMicroseconds;
            Console.WriteLine($"{payload.GlobalSeqNo} ping echoed on connection {payload.ConnectionId}, " +
                              $"round trip {roundTripUs}us");
        }
    }

    // This logical gateway's connections, whichever instance opened them: how a standby that keeps per-connection
    // state rebuilds it while it replays.
    public void OnConnectionOpened(int connectionId, IDirectBuffer connectionData, int offset, int length)
    {
        Console.WriteLine($"# connection {connectionId} opened " +
                          $"({connectionData.GetStringWithoutLengthAscii(offset, length)})");
    }

    public void OnConnectionClosed(int connectionId)
    {
        Console.WriteLine($"# connection {connectionId} closed");
    }

    public void OnCaughtUp(long globalSeqNo)
    {
        Console.WriteLine($"# caught up at {globalSeqNo} — following the tap live");
    }

    public void OnClusterHeartbeat(long clusterTimeNs, long receiveTimeNs)
    {
        // Nothing to time here; a gateway with a watchdog runs it off this.
    }

    // Latched, once: this instance may no longer act. Exiting releases its session, so the standby takes over.
    public void OnFenced(ClusterError fence, string detail)
    {
        _fault = fence + ": " + detail;
    }

    private static int EnvInt(string name, int fallback)
    {
        string value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrEmpty(value) ? fallback : int.Parse(value);
    }
}
