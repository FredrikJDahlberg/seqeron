package org.limitless.seqeron.app;

import io.aeron.Aeron;
import java.util.Objects;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.util.Clocks;

/**
 * The simplest way to follow a queue: it reads one group's queue and publishes nothing, so it opens no cluster
 * session, takes no part in an election and needs no topology row. It replays local history through the co-located
 * Replayer, switches to the live queue once caught up, and delivers every application message once, in
 * {@code clusterSeqNo} order, as a {@link Payload} — the same view the producers' façades give. Anything more is an
 * {@link Application}'s.
 *
 * <p>Fenced, like them, when it can no longer be trusted to see the queue: {@link ClusterError#RECOVERY_STALLED}
 * and {@link ClusterError#QUEUE_STALLED}. It takes no snapshots and so always starts from the start of its queue: a
 * queue under a {@code <snapshots>} policy fences it with {@link ClusterError#SNAPSHOTTED_QUEUE} at its first sign of
 * one, the policy's row, a round's {@code SnapshotStarted} or a queue that starts at a cut.
 *
 * <p>Single-threaded: every method belongs to the caller's one duty-cycle thread, which calls {@link #doWork()}
 * each iteration. {@link ReplayerStreamReceiver} is the lower-level path this sits on.
 */
public final class Consumer implements AutoCloseable {
    /** What a consumer does that this class cannot do for it. */
    public interface Listener {
        /** One application payload off the queue, in {@code clusterSeqNo} order. */
        void onSequenced(Payload payload);

        /** Every transition to caught-up, the first included. */
        default void onCaughtUp(final long clusterSeqNo) {
        }

        /**
         * The cluster clock's tick (spec §7), once a second. It is identical on every node, so a timer driven by it
         * decides the same thing everywhere.
         */
        default void onClusterHeartbeat(final long clusterTimeNs, final long receiveTimeNs) {
        }

        /** Once, latched: this consumer may no longer be trusted to see the queue. Exiting is the usual way. */
        void onFenced(ClusterError fence, String detail);
    }

    private final Listener listener;
    private final int memberId;
    private final int groupId;
    private final long recoveryStallTimeoutMs;
    private final long queueStallTimeoutMs;
    private final ReplayerStreamReceiver receiver;
    private final RecoveryStallFence recoveryStall;
    private final QueueStallFence queueStall;
    private final Payload payload = new Payload();
    private boolean caughtUp;
    private boolean fenced;
    /** The queue showed a snapshot frame; fenced from {@link #doWork()}, outside the receiver's poll. */
    private boolean snapshotted;

    private Consumer(final Builder builder) {
        this.listener = builder.listener;
        this.memberId = builder.memberId;
        this.groupId = builder.groupId;
        this.queueStallTimeoutMs = builder.tuning.queueStallTimeoutMs();
        this.recoveryStallTimeoutMs = builder.tuning.recoveryStallTimeoutMs();
        this.recoveryStall = new RecoveryStallFence(recoveryStallTimeoutMs);
        this.queueStall = new QueueStallFence(queueStallTimeoutMs);
        this.receiver = new ReplayerStreamReceiver(builder.clientId, this::onSequenced, null, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Starts following the group's queue on this node; the Aeron client is the caller's. */
    public void start(final Aeron aeron) {
        receiver.start(aeron, memberId, groupId);
    }

    /**
     * One duty-cycle iteration. Nothing once fenced.
     * @return units of work done, for the caller's idle strategy
     */
    public int doWork() {
        if (fenced) {
            return 0;
        }
        final int work = receiver.poll();
        checkSnapshotted();
        if (fenced) {
            return work;
        }
        checkCaughtUp();
        if (!fenced) {
            checkFences();
        }
        return work;
    }

    public boolean isCaughtUp() {
        return receiver.isCaughtUp();
    }

    public long lastClusterSeqNo() {
        return receiver.lastClusterSeqNo();
    }

    /** Closes the queue and replay subscriptions. The Aeron client is the caller's and is left open. */
    @Override
    public void close() {
        receiver.close();
    }

    void onSequenced(final SequencedEvent event) {
        if (snapshotted) {
            return;
        }
        if (event.isSystem()) {
            final int systemEventType = event.systemEventType();
            if (systemEventType == SystemFrame.SNAPSHOT_POLICY_REGISTERED ||
                systemEventType == SystemFrame.SNAPSHOT_STARTED) {
                snapshotted = true;
                return;
            }
            if (systemEventType == SystemFrame.CLUSTER_HEARTBEAT) {
                queueStall.onClusterHeartbeat(Clocks.monotonicMs());
                listener.onClusterHeartbeat(event.clusterTimestampNs(), event.receiveTimeNs());
            }
            return;
        }
        payload.wrap(event);
        listener.onSequenced(payload);
    }

    /** A queue under a snapshot policy is an {@link Application}'s to follow. */
    void checkSnapshotted() {
        if (receiver.isQueueStartMissing()) {
            fence(ClusterError.SNAPSHOTTED_QUEUE,
                  "this node's queue starts at a cluster snapshot's cut; follow it with an Application");
        } else if (snapshotted) {
            fence(ClusterError.SNAPSHOTTED_QUEUE,
                  "the queue is under a <snapshots> policy; follow it with an Application");
        }
    }

    /** Polled rather than taken off the receiver's callback: neither fence may wait on the next frame to land. */
    private void checkCaughtUp() {
        final boolean now = receiver.isCaughtUp();
        if (now == caughtUp) {
            return;
        }
        caughtUp = now;
        if (now) {
            recoveryStall.onCaughtUp();
            queueStall.onCaughtUp(Clocks.monotonicMs());
            listener.onCaughtUp(receiver.lastClusterSeqNo());
        }
    }

    private void checkFences() {
        final long nowMs = Clocks.monotonicMs();
        if (!receiver.isCaughtUp()) {
            if (recoveryStall.onNotCaughtUp(nowMs, receiver.lastClusterSeqNo())) {
                fence(ClusterError.RECOVERY_STALLED, "recovery has dispatched nothing for >" + recoveryStallTimeoutMs +
                      "ms (clusterSeqNo stuck at " + receiver.lastClusterSeqNo() + ")");
            }
        } else if (queueStall.isStalled(nowMs)) {
            fence(ClusterError.QUEUE_STALLED, "no ClusterHeartbeat for >" + queueStallTimeoutMs + "ms");
        }
    }

    /** Releases the subscriptions first, so a fenced consumer holds no Replayer slot while its listener hears why. */
    private void fence(final ClusterError reason, final String detail) {
        fenced = true;
        receiver.close();
        listener.onFenced(reason, detail);
    }

    /** Everything one consumer needs to follow its queue. */
    public static final class Builder {
        private Integer clientId;
        private Integer memberId;
        private Integer groupId;
        private Listener listener;
        private Tuning tuning = Tuning.DEFAULT;

        /**
         * This consumer's client id, its counters' label — required, and unique among the clients on its node: two
         * sharing one share their counters' series.
         */
        public Builder clientId(final int clientId) {
            this.clientId = clientId;
            return this;
        }

        /** The node whose queue this consumer follows, or a gateway host's node id — required. */
        public Builder memberId(final int memberId) {
            this.memberId = memberId;
            return this;
        }

        /** The group whose queue this consumer reads — required; the first topology's is 1. */
        public Builder groupId(final int groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder listener(final Listener listener) {
            this.listener = listener;
            return this;
        }

        /** The stall policy, {@link Tuning#DEFAULT} by default; {@code pendingCapacity} is unused, as it sends nothing. */
        public Builder tuning(final Tuning tuning) {
            this.tuning = Objects.requireNonNull(tuning, "tuning");
            return this;
        }

        public Consumer build() {
            Objects.requireNonNull(clientId, "clientId");
            Objects.requireNonNull(memberId, "memberId");
            Objects.requireNonNull(groupId, "groupId");
            Objects.requireNonNull(listener, "listener");
            return new Consumer(this);
        }
    }
}
