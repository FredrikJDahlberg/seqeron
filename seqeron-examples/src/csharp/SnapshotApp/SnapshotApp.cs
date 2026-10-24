using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.App;

namespace Example;

/// <summary>
/// A co-located application whose state survives its restart through a snapshot, and the C# twin of
/// <c>SnapshotApp.java</c>. The state is a ledger: eight accounts that open with 1000 each, and the transfers between
/// them. The leading replica submits a random transfer once a second; every replica applies each one in log order,
/// and one that would overdraw its source account is rejected — so what a transfer does depends on every transfer
/// before it, and a restart must restore the balances exactly. A restart restores the newest round the log confirms
/// and applies only the transfers after its cut.
/// <para>Rounds need this application's <c>snapshot="true"</c> row and a <c>&lt;snapshots&gt;</c> policy, which
/// <c>csharp-client-test.sh</c> adds to <c>seqeron-examples/topology-csharp.xml</c>. Environment:
/// <c>SEQERON_NODE_MEMBER_ID</c> (default 0), <c>SEQERON_REPLAYER_CLIENT_ID</c> (default 23),
/// <c>SEQERON_AERON_DIR</c>, <c>SEQERON_EXAMPLE_SNAPSHOT_DIR</c>.</para>
/// </summary>
public sealed class SnapshotApp : IApplicationListener, ISnapshotListener
{
    // A transfer: from int32, to int32, amount int64, little-endian, no schema.
    private const int TransferPayloadId = 7;
    private const int TransferLength = 2 * sizeof(int) + sizeof(long);

    // This example's own sourceId (spec §5); Java's is 14 and C++'s 15.
    private const int SourceId = 19;

    private const int Accounts = 8;
    private const long OpeningBalance = 1000;
    private const int MaxAmount = 400;

    // Raised when a record's layout changes. A restore stops on a file of another one, unless SupportsFormatVersion
    // is overridden to accept it and OnRestore reads it.
    private const int SnapshotFormatVersion = 1;

    private const string EgressChannel = "aeron:udp?endpoint=localhost:0";

    // Agrona.NET's backoff parks for up to 16 ms by default; Java's for at most 1 ms, which this keeps.
    private const long MaxParkPeriodMs = 1;

    private static readonly long TransferIntervalTicks = Stopwatch.Frequency;

    // Fenced, restart me: this repository's convention (doc/ops.md).
    private const int ExitFenced = 70;

    // The replicated state: derived from the log alone, so identical on every replica.
    private readonly long[] _balances = Enumerable.Repeat(OpeningBalance, Accounts).ToArray();
    private long _applied;
    private long _rejected;

    private readonly UnsafeBuffer _transfer = new UnsafeBuffer(new byte[TransferLength]);
    private readonly Random _random = new Random();
    private Application _app;
    private volatile bool _running = true;
    private string _fence;

    public static int Main()
    {
        int memberId = EnvInt("SEQERON_NODE_MEMBER_ID", 0);
        int clientId = EnvInt("SEQERON_REPLAYER_CLIENT_ID", 23);
        // The member's own, as PortLayout.AeronDir names it, which this façade-only example may not name.
        string aeronDir = Environment.GetEnvironmentVariable("SEQERON_AERON_DIR") ??
                          Path.Combine(Directory.Exists("/dev/shm") ? "/dev/shm" : Path.GetTempPath(),
                                       "seqeron-seq-aeron-" + memberId);
        // The replica's own, and it must outlive the process: a restart restores from it.
        string snapshotDir = Environment.GetEnvironmentVariable("SEQERON_EXAMPLE_SNAPSHOT_DIR") ??
                             Path.Combine(Path.GetTempPath(), $"seqeron-example-snapshots-{SourceId}-{memberId}");

        var application = new SnapshotApp();
        using PosixSignalRegistration sigint = PosixSignalRegistration.Create(PosixSignal.SIGINT, application.Stop);
        using PosixSignalRegistration sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, application.Stop);

        using Aeron aeron = Aeron.Connect(new Aeron.Context().AeronDirectoryName(aeronDir));
        using var app =
            new Application(new ApplicationOptions { SourceId = SourceId, ClientId = clientId, MemberId = memberId,
                                                     EgressChannel = EgressChannel, Listener = application,
                                                     SnapshotListener = application, SnapshotDirectory = snapshotDir });
        application._app = app;
        app.Start(aeron);
        Console.WriteLine($"# member {memberId} via {aeronDir} — snapshots in {snapshotDir}");
        application.Run();
        if (application._fence != null)
        {
            Console.Error.WriteLine("# " + application._fence);
            return ExitFenced;
        }
        return 0;
    }

    private void Run()
    {
        var idle = new BackoffIdleStrategy(Configuration.IDLE_MAX_SPINS, Configuration.IDLE_MAX_YIELDS,
                                           Configuration.IDLE_MIN_PARK_MS, MaxParkPeriodMs);
        long nextTransferTicks = 0;
        while (_running && _fence == null)
        {
            int work = _app.DoWork();
            long now = Stopwatch.GetTimestamp();
            if (_app.CanPublish && now >= nextTransferTicks)
            {
                SubmitTransfer();
                nextTransferTicks = now + TransferIntervalTicks;
            }
            idle.Idle(work);
        }
    }

    private void Stop(PosixSignalContext context)
    {
        context.Cancel = true;
        _running = false;
    }

    // A command, not a state change: the randomness is the leader's alone, and the replicas see only the payload.
    // Whether it overdraws is decided when it is applied, not here. A declined one is not retried.
    private void SubmitTransfer()
    {
        int from = _random.Next(Accounts);
        int to = (from + 1 + _random.Next(Accounts - 1)) % Accounts;
        _transfer.PutInt(0, from, ByteOrder.LittleEndian);
        _transfer.PutInt(sizeof(int), to, ByteOrder.LittleEndian);
        _transfer.PutLong(2 * sizeof(int), 1 + _random.Next(MaxAmount), ByteOrder.LittleEndian);
        _app.Publish(TransferPayloadId, _transfer, TransferLength);
    }

    // The state transition: every replica applies the same transfers in the same order.
    public void OnSequenced(Payload payload)
    {
        if (payload.SourceId != SourceId || payload.PayloadId != TransferPayloadId ||
            payload.PayloadLength != TransferLength)
        {
            return;
        }
        IDirectBuffer buffer = payload.Buffer;
        int offset = payload.PayloadOffset;
        int from = buffer.GetInt(offset, ByteOrder.LittleEndian);
        int to = buffer.GetInt(offset + sizeof(int), ByteOrder.LittleEndian);
        long amount = buffer.GetLong(offset + 2 * sizeof(int), ByteOrder.LittleEndian);
        bool ok = _balances[from] >= amount;
        if (ok)
        {
            _balances[from] -= amount;
            _balances[to] += amount;
            _applied++;
        }
        else
        {
            _rejected++;
        }
        Console.WriteLine($"{payload.ClusterSeqNo} transfer {amount} from {from} to {to} " +
                          $"{(ok ? "applied" : "rejected")} {Balances()}");
    }

    public int FormatVersion => SnapshotFormatVersion;

    // Record 0 holds the counters, applied and rejected int64; records 1 to 8 one balance each, int64 in account
    // order. A ledger of many accounts is why a snapshot is a sequence of records rather than one: each record is at
    // most 65535 bytes, and the number of them is unbounded.
    public int OnSnapshot(IMutableDirectBuffer buffer, int recordIndex)
    {
        if (recordIndex == 0)
        {
            buffer.PutLong(0, _applied, ByteOrder.LittleEndian);
            buffer.PutLong(sizeof(long), _rejected, ByteOrder.LittleEndian);
            Console.WriteLine($"# snapshot after {_applied} transfers, {_rejected} rejected");
            return 2 * sizeof(long);
        }
        if (recordIndex > Accounts)
        {
            return 0;
        }
        buffer.PutLong(0, _balances[recordIndex - 1], ByteOrder.LittleEndian);
        return sizeof(long);
    }

    public void OnRestore(IDirectBuffer buffer, int length, int recordIndex, int formatVersion)
    {
        if (recordIndex == 0)
        {
            _applied = buffer.GetLong(0, ByteOrder.LittleEndian);
            _rejected = buffer.GetLong(sizeof(long), ByteOrder.LittleEndian);
            return;
        }
        _balances[recordIndex - 1] = buffer.GetLong(0, ByteOrder.LittleEndian);
        if (recordIndex == Accounts)
        {
            Console.WriteLine($"# restored after {_applied} transfers, {_rejected} rejected {Balances()}");
        }
    }

    public void OnGroupsResolved(GroupDirectory directory)
    {
    }

    public void OnLeadershipChanged(bool leading)
    {
        Console.WriteLine(leading ? "# leading — submitting transfers" : "# not leading — silent");
    }

    // Transfers move money and never make it, so the total is the opening one whatever was restored.
    public void OnCaughtUp(long clusterSeqNo)
    {
        Console.WriteLine($"# caught up at {clusterSeqNo} — {_applied} transfers, {_rejected} rejected, " +
                          $"total {_balances.Sum()}");
    }

    public void OnClusterHeartbeat(long clusterTimeNs, long receiveTimeNs)
    {
    }

    // SnapshotDiverged and SnapshotUnrestorable arrive here like every other fence.
    public void OnFenced(ClusterError fence, string detail)
    {
        _fence = fence + ": " + detail;
    }

    private string Balances()
    {
        return "[" + string.Join(", ", _balances) + "]";
    }

    private static int EnvInt(string name, int fallback)
    {
        string value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrEmpty(value) ? fallback : int.Parse(value);
    }
}
