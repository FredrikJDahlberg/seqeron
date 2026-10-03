package example;

import io.aeron.Aeron;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.app.Application;
import org.limitless.seqeron.app.ClusterError;
import org.limitless.seqeron.app.Payload;
import org.limitless.seqeron.app.SnapshotListener;

/**
 * A co-located application whose state survives its restart through a snapshot. The state is a ledger: eight
 * accounts that open with 1000 each, and the transfers between them. The leading replica submits a random
 * transfer once a second; every replica applies each one in log order, and one that would overdraw its source
 * account is rejected — so what a transfer does depends on every transfer before it, and a restart must
 * restore the balances exactly. A restart restores the newest round the log confirms and applies only the
 * transfers after its cut.
 *
 * <p>Rounds need this application's {@code snapshot="true"} row and a {@code <snapshots>} policy, both in
 * {@code seqeron-examples/topology.xml}. Properties: {@code -Dsnapshot.member} (default 0),
 * {@code -Dsnapshot.clientId} (default 17), {@code -Dsnapshot.aeronDir}, {@code -Dsnapshot.dir}.
 */
public final class SnapshotApp implements Application.Listener, SnapshotListener {
    /** A transfer: {@code from} int32, {@code to} int32, {@code amount} int64, little-endian, no schema. */
    private static final int TRANSFER_PAYLOAD_ID = 7;
    private static final int TRANSFER_LENGTH = 2 * Integer.BYTES + Long.BYTES;
    private static final int SOURCE_ID = 14;

    private static final int ACCOUNTS = 8;
    private static final long OPENING_BALANCE = 1000;
    private static final int MAX_AMOUNT = 400;

    /** Raised when a record's layout changes; a restore stops on a file of another one. */
    private static final int FORMAT_VERSION = 1;

    private static final long TRANSFER_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);
    private static final String EGRESS_CHANNEL = "aeron:udp?endpoint=localhost:0";

    // The replicated state: derived from the log alone, so identical on every replica.
    private final long[] balances = new long[ACCOUNTS];
    private long applied;
    private long rejected;

    private final MutableDirectBuffer transfer = new UnsafeBuffer(new byte[TRANSFER_LENGTH]);
    private final Random random = new Random();
    private Application app;
    private String fence;

    private SnapshotApp() {
        Arrays.fill(balances, OPENING_BALANCE);
    }

    public static void main(final String[] args) {
        final int memberId = Integer.getInteger("snapshot.member", 0);
        final int clientId = Integer.getInteger("snapshot.clientId", 17);
        final String tmpDir = System.getProperty("java.io.tmpdir");
        final String aeronDir = System.getProperty("snapshot.aeronDir", tmpDir + "/seqeron-seq-aeron-" + memberId);
        // The replica's own, and it must outlive the process: a restart restores from it.
        final Path snapshotDir = Path.of(System.getProperty(
            "snapshot.dir", tmpDir + "/seqeron-example-snapshots-" + SOURCE_ID + "-" + memberId));

        final AtomicBoolean running = new AtomicBoolean(true);
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            try {
                mainThread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }));

        final SnapshotApp application = new SnapshotApp();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
             Application app = Application.builder()
                 .sourceId(SOURCE_ID).clientId(clientId).memberId(memberId)
                 .egressChannel(EGRESS_CHANNEL).listener(application)
                 .snapshotListener(application).snapshotDirectory(snapshotDir)
                 .build()) {

            application.app = app;
            app.start(aeron);
            System.out.printf("# member %d via %s — snapshots in %s%n", memberId, aeronDir, snapshotDir);
            application.run(running);
        }
        System.out.flush();
        if (application.fence != null) {
            System.err.println("# " + application.fence);
            System.exit(1);
        }
    }

    private void run(final AtomicBoolean running) {
        final IdleStrategy idle = new BackoffIdleStrategy();
        long nextTransferNs = 0;
        while (running.get() && fence == null) {
            final int work = app.doWork();
            final long now = System.nanoTime();
            if (app.canPublish() && now >= nextTransferNs) {
                submitTransfer();
                nextTransferNs = now + TRANSFER_INTERVAL_NS;
            }
            idle.idle(work);
        }
    }

    /**
     * A command, not a state change: the randomness is the leader's alone, and the replicas see only the
     * payload. Whether it overdraws is decided when it is applied, not here. A declined one is not retried.
     */
    private void submitTransfer() {
        final int from = random.nextInt(ACCOUNTS);
        final int to = (from + 1 + random.nextInt(ACCOUNTS - 1)) % ACCOUNTS;
        transfer.putInt(0, from, ByteOrder.LITTLE_ENDIAN);
        transfer.putInt(Integer.BYTES, to, ByteOrder.LITTLE_ENDIAN);
        transfer.putLong(2 * Integer.BYTES, 1 + random.nextInt(MAX_AMOUNT), ByteOrder.LITTLE_ENDIAN);
        app.publish(TRANSFER_PAYLOAD_ID, transfer, TRANSFER_LENGTH);
    }

    /** The state transition: every replica applies the same transfers in the same order. */
    @Override
    public void onSequenced(final Payload payload) {
        if (payload.sourceId() != SOURCE_ID || payload.payloadId() != TRANSFER_PAYLOAD_ID
            || payload.payloadLength() != TRANSFER_LENGTH) {
            return;
        }
        final DirectBuffer buffer = payload.buffer();
        final int offset = payload.payloadOffset();
        final int from = buffer.getInt(offset, ByteOrder.LITTLE_ENDIAN);
        final int to = buffer.getInt(offset + Integer.BYTES, ByteOrder.LITTLE_ENDIAN);
        final long amount = buffer.getLong(offset + 2 * Integer.BYTES, ByteOrder.LITTLE_ENDIAN);
        final boolean ok = balances[from] >= amount;
        if (ok) {
            balances[from] -= amount;
            balances[to] += amount;
            applied++;
        } else {
            rejected++;
        }
        System.out.printf("%d transfer %d from %d to %d %s %s%n", payload.globalSeqNo(), amount, from, to,
                          ok ? "applied" : "rejected", Arrays.toString(balances));
    }

    @Override
    public int formatVersion() {
        return FORMAT_VERSION;
    }

    /**
     * Record 0 holds the counters, {@code applied} and {@code rejected} int64; records 1 to 8 one balance each,
     * int64 in account order. A ledger of many accounts is why a snapshot is a sequence of records rather than
     * one: each record is at most 65535 bytes, and the number of them is unbounded.
     */
    @Override
    public int onSnapshot(final MutableDirectBuffer buffer, final int recordIndex) {
        if (recordIndex == 0) {
            buffer.putLong(0, applied, ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(Long.BYTES, rejected, ByteOrder.LITTLE_ENDIAN);
            System.out.printf("# snapshot after %d transfers, %d rejected%n", applied, rejected);
            return 2 * Long.BYTES;
        }
        if (recordIndex > ACCOUNTS) {
            return 0;
        }
        buffer.putLong(0, balances[recordIndex - 1], ByteOrder.LITTLE_ENDIAN);
        return Long.BYTES;
    }

    @Override
    public void onRestore(final DirectBuffer buffer, final int length, final int recordIndex) {
        if (recordIndex == 0) {
            applied = buffer.getLong(0, ByteOrder.LITTLE_ENDIAN);
            rejected = buffer.getLong(Long.BYTES, ByteOrder.LITTLE_ENDIAN);
            return;
        }
        balances[recordIndex - 1] = buffer.getLong(0, ByteOrder.LITTLE_ENDIAN);
        if (recordIndex == ACCOUNTS) {
            System.out.printf("# restored after %d transfers, %d rejected %s%n", applied, rejected,
                              Arrays.toString(balances));
        }
    }

    @Override
    public void onLeadershipChanged(final boolean leading) {
        System.out.println(leading ? "# leading — submitting transfers" : "# not leading — silent");
    }

    /** Transfers move money and never make it, so the total is the opening one whatever was restored. */
    @Override
    public void onCaughtUp(final long globalSeqNo) {
        System.out.printf("# caught up at %d — %d transfers, %d rejected, total %d%n", globalSeqNo, applied,
                          rejected, Arrays.stream(balances).sum());
    }

    @Override
    public void onClusterHeartbeat(final long clusterTimeNs, final long receiveTimeNs) {
    }

    /** SNAPSHOT_DIVERGED and SNAPSHOT_UNRESTORABLE arrive here like every other fence. */
    @Override
    public void onFenced(final ClusterError fence, final String detail) {
        this.fence = fence + ": " + detail;
    }
}
