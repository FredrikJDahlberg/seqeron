package org.limitless.seqeron.metrics;

import io.aeron.Aeron;
import io.aeron.Counter;
import java.nio.charset.StandardCharsets;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Registry of custom Aeron counter type ids used by seqeron's own processes, mirroring {@link
 * io.aeron.AeronCounters}'s registry pattern. Aeron reserves typeId 0-999 for itself (client/driver
 * 0-99, archive 100-199, cluster 200-299); custom counters must use 1000 or higher. One block per
 * process family so a range scan (see {@code clusterctl counters}) can find them all without listing
 * individual ids.
 */
public final class SeqeronCounters {
    // SequencerService (the Aeron Cluster service)
    public static final int SEQUENCER_TYPE_ID_MIN = 5000;
    public static final int SEQUENCER_TYPE_ID_MAX = 5099;

    /** Mirrors {@code Sequencer.globalSeqNo()} — the last globalSeqNo emitted on this node's tap. */
    public static final int SEQUENCER_GLOBAL_SEQ_NO_TYPE_ID = 5000;

    /** Count of times the tap-emit back-pressure alert threshold has fired. */
    public static final int SEQUENCER_TAP_BACKPRESSURE_ALERTS_TYPE_ID = 5001;

    /** Count of malformed ingress messages skipped by {@code Sequencer.sequenceMessage}. */
    public static final int SEQUENCER_REJECTED_INGRESS_COUNT_TYPE_ID = 5002;

    /** Count of leadership changes this node has observed and sequenced. */
    public static final int SEQUENCER_LEADERSHIP_CHANGE_COUNT_TYPE_ID = 5003;

    /** memberId of the leader last recorded by this node's {@code Sequencer}. */
    public static final int SEQUENCER_CURRENT_LEADER_MEMBER_ID_TYPE_ID = 5004;

    /** Consensus timestamp of the last 1 Hz {@code ClusterHeartbeat} emitted. */
    public static final int SEQUENCER_LAST_CLUSTER_HEARTBEAT_TIMESTAMP_TYPE_ID = 5005;

    /** Count of standby-promotion {@code GatewayActive} frames emitted on a gateway session close. */
    public static final int SEQUENCER_GATEWAY_PROMOTION_COUNT_TYPE_ID = 5006;

    /** 1 once the bootstrap {@code GatewayActive} has been emitted for the trading day, else 0. */
    public static final int SEQUENCER_BOOTSTRAP_ACTIVATED_TYPE_ID = 5007;

    /**
     * 1 while tap-emit back-pressure has been sustained past {@code SequencerService}'s stall
     * threshold — distinguishes a genuine local-archive stall from the ordinary, self-clearing
     * back-pressure {@link #SEQUENCER_TAP_BACKPRESSURE_ALERTS_TYPE_ID} already alerts on — else 0.
     */
    public static final int SEQUENCER_TAP_STALLED_TYPE_ID = 5008;

    /** Current count of TCP clients connected across every gateway, derived from the sequenced log. */
    public static final int SEQUENCER_CONNECTED_CLIENTS_TYPE_ID = 5009;

    /** Count of ingress messages successfully sequenced; {@code rate()} over this is ingress throughput. */
    public static final int SEQUENCER_INGRESS_MESSAGES_TYPE_ID = 5010;

    /**
     * Count of gateway session closes that found no standby to promote ({@code
     * Sequencer.NO_PROMOTION_TARGET}): the cluster is left with no active instance of that logical
     * gateway until one starts. Distinct from a session close that was never a gateway's at all, which
     * this counter does not see.
     */
    public static final int SEQUENCER_GATEWAY_PROMOTION_FAILED_COUNT_TYPE_ID = 5011;

    // ReplayerService (per-node replay server)
    public static final int REPLAYER_TYPE_ID_MIN = 5100;
    public static final int REPLAYER_TYPE_ID_MAX = 5199;

    /** 1 while the local archive is unreachable for replay (live delivery is unaffected), else 0. */
    public static final int REPLAYER_STALLED_TYPE_ID = 5100;

    /** 1 once the co-located tap recording is visible and replay requests are being served, else 0. */
    public static final int REPLAYER_READY_TYPE_ID = 5101;

    /** Current count of in-flight replays, out of {@code MAX_CONCURRENT_REPLAYS}. */
    public static final int REPLAYER_ACTIVE_SLOTS_TYPE_ID = 5102;

    /** Current count of replay requests waiting for a free slot. */
    public static final int REPLAYER_PENDING_REQUESTS_TYPE_ID = 5103;

    /** Count of replays started ({@code archive.startReplay}) since this node came up. */
    public static final int REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID = 5104;

    /** Count of replay slots reclaimed by the idle-TTL backstop rather than a normal release. */
    public static final int REPLAYER_IDLE_TTL_RECLAIMED_COUNT_TYPE_ID = 5105;

    /**
     * 1 once this node's oldest tap recording has failed the startup integrity check (its first frame
     * is not globalSeqNo 1 — the recording doesn't reach the start of the log), else 0. Latched: once
     * set, {@code ready} never becomes 1 for this process's lifetime.
     */
    public static final int REPLAYER_INTEGRITY_FAILURE_TYPE_ID = 5106;

    /**
     * Count of control replies dropped rather than spun on because an app stopped draining the control
     * stream. Each costs that app one resend interval and nothing else; a rising rate means a wedged
     * replica, and is the only trace of it — the Replayer no longer stalls in sympathy.
     */
    public static final int REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID = 5107;

    /**
     * 1 once two co-located apps have been seen sharing one {@code SEQERON_REPLAYER_CLIENT_ID} (see
     * {@code ReplayClientIdCollisions}), else 0. A configuration fault, not a runtime one: the two
     * stop each other's replays on every request and neither ever catches up, so a node showing this
     * has replicas that will not recover until it is corrected.
     */
    public static final int REPLAYER_CLIENT_ID_COLLISION_TYPE_ID = 5108;

    // ── Co-located application replicas (5200-5299) ────────────────────────────────────────────
    // Core reserves 5200 and nothing else in the range; a consumer allocates its own inside it
    // (doc/registries.md §3), which is also why MetricsExporter names an unrecognised app counter from
    // its label rather than from a table here.
    // Published by the apps themselves, not by any Java process — see the C++ half in
    // {@code util/SeqeronCounters.hpp}, which must be kept in step with the ids and key layout here.
    public static final int APP_TYPE_ID_MIN = 5200;
    public static final int APP_TYPE_ID_MAX = 5299;

    /**
     * 1 while a co-located replica's recovery has been reported unconvergent — nothing dispatched for
     * {@code RECOVERY_PROGRESS_TIMEOUT_MS} while not caught up (see {@code RecoveryProgressPolicy}) —
     * else 0. The replica is holding, which is correct and safe; what it is not is serving.
     */
    public static final int APP_RECOVERY_STALLED_TYPE_ID = 5200;

    /** Whole range this class owns, for a typeId-range scan (see {@code clusterctl counters}). */
    public static final int MIN_TYPE_ID = SEQUENCER_TYPE_ID_MIN;
    public static final int MAX_TYPE_ID = APP_TYPE_ID_MAX;

    /** Offset of the memberId int within a counter's key buffer (see {@link #addCounter}). */
    public static final int KEY_MEMBER_ID_OFFSET = 0;

    /**
     * Offset of the replayer clientId int in an <b>app</b> counter's key. Node-scoped counters carry the
     * memberId alone, but a node runs several co-located replicas publishing the same type id, so
     * without this they would all render as one Prometheus series per node — the same label set,
     * silently overwritten. Only counters in the {@link #APP_TYPE_ID_MIN}..{@link #APP_TYPE_ID_MAX}
     * range carry it; on the others this offset reads 0 out of Aeron's zero-filled key area, which is
     * why the exporter keys off the range rather than probing the value.
     */
    public static final int KEY_CLIENT_ID_OFFSET = BitUtil.SIZE_OF_INT;

    /** Length of the key buffer written by {@link #addCounter}: one int, the memberId. */
    public static final int KEY_LENGTH = BitUtil.SIZE_OF_INT;

    /** Length of the key buffer written by {@link #addAppCounter}: memberId then clientId. */
    public static final int APP_KEY_LENGTH = 2 * BitUtil.SIZE_OF_INT;

    /**
     * Allocates a seqeron operator counter with {@code memberId} encoded as a 4-byte int key
     * (offset {@link #KEY_MEMBER_ID_OFFSET}) alongside its human-readable label, so a remote reader
     * (e.g. a Prometheus exporter reading {@code CountersReader.forEach(MetaData)}) can recover the
     * member as structured data instead of parsing it back out of the label text.
     */
    public static Counter addCounter(final Aeron aeron, final int typeId, final String label, final int memberId) {
        final UnsafeBuffer keyBuffer = new UnsafeBuffer(new byte[KEY_LENGTH]);
        keyBuffer.putInt(KEY_MEMBER_ID_OFFSET, memberId);
        final byte[] labelBytes = label.getBytes(StandardCharsets.US_ASCII);
        final UnsafeBuffer labelBuffer = new UnsafeBuffer(labelBytes);
        return aeron.addCounter(typeId, keyBuffer, 0, KEY_LENGTH, labelBuffer, 0, labelBytes.length);
    }

    /**
     * Allocates an <b>app</b> counter, keyed on {@code {memberId, clientId}} rather than the memberId alone
     * (see {@link #KEY_CLIENT_ID_OFFSET}): a node runs several co-located replicas publishing the same type
     * id, and without the clientId they all render as one series per node. The C++ twin is
     * {@code util/SeqeronCounters.hpp}'s {@code addAppCounter}; the key layout must match.
     */
    public static Counter addAppCounter(final Aeron aeron, final int typeId, final String label, final int memberId,
                                        final int clientId) {
        final UnsafeBuffer keyBuffer = new UnsafeBuffer(new byte[APP_KEY_LENGTH]);
        keyBuffer.putInt(KEY_MEMBER_ID_OFFSET, memberId);
        keyBuffer.putInt(KEY_CLIENT_ID_OFFSET, clientId);
        final byte[] labelBytes = label.getBytes(StandardCharsets.US_ASCII);
        final UnsafeBuffer labelBuffer = new UnsafeBuffer(labelBytes);
        return aeron.addCounter(typeId, keyBuffer, 0, APP_KEY_LENGTH, labelBuffer, 0, labelBytes.length);
    }

    private SeqeronCounters() {
    }
}
