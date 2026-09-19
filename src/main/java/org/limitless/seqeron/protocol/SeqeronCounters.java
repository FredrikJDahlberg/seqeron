package org.limitless.seqeron.protocol;

import io.aeron.Aeron;
import io.aeron.Counter;
import java.nio.charset.StandardCharsets;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Registry of seqeron's custom Aeron counter type ids (Aeron reserves 0-999). One block per process family,
 * so a range scan ({@code clusterctl counters}) finds them all.
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

    /** 1 while tap back-pressure has lasted past the stall threshold with no recording progress, else 0. */
    public static final int SEQUENCER_TAP_STALLED_TYPE_ID = 5008;

    /** Current count of TCP clients connected across every gateway, derived from the sequenced log. */
    public static final int SEQUENCER_CONNECTED_CLIENTS_TYPE_ID = 5009;

    /** Count of ingress messages successfully sequenced; {@code rate()} over this is ingress throughput. */
    public static final int SEQUENCER_INGRESS_MESSAGES_TYPE_ID = 5010;

    /** Count of gateway session closes that found no standby to promote, leaving that gateway with none active. */
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
     * 1 once a tap recording has failed the startup integrity check (its first frame is not globalSeqNo 1),
     * else 0. Latched: {@code ready} never becomes 1 again for this process.
     */
    public static final int REPLAYER_INTEGRITY_FAILURE_TYPE_ID = 5106;

    /**
     * Count of control replies dropped because an app stopped draining the control stream; each costs that
     * app one resend interval. A rising rate means a wedged replica.
     */
    public static final int REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID = 5107;

    /**
     * 1 once two co-located apps have been seen sharing one {@code SEQERON_REPLAYER_CLIENT_ID}, else 0. Their
     * replicas stop each other's replays and will not recover until the configuration is corrected.
     */
    public static final int REPLAYER_CLIENT_ID_COLLISION_TYPE_ID = 5108;

    // ── Co-located application replicas (5200-5299) ────────────────────────────────────────────
    // Core reserves 5200; a consumer allocates its own in the range (doc/registries.md §3). Published by
    // the apps, not by any Java process: protocol/SeqeronCounters.hpp must match these ids and key layout.
    public static final int APP_TYPE_ID_MIN = 5200;
    public static final int APP_TYPE_ID_MAX = 5299;

    /** 1 while a co-located replica's recovery has dispatched nothing while not caught up, else 0. */
    public static final int APP_RECOVERY_STALLED_TYPE_ID = 5200;

    /** Whole range this class owns, for a typeId-range scan (see {@code clusterctl counters}). */
    public static final int MIN_TYPE_ID = SEQUENCER_TYPE_ID_MIN;
    public static final int MAX_TYPE_ID = APP_TYPE_ID_MAX;

    /** Offset of the memberId int within a counter's key buffer (see {@link #addCounter}). */
    public static final int KEY_MEMBER_ID_OFFSET = 0;

    /**
     * Offset of the replayer clientId in an <b>app</b> counter's key, so a node's several replicas render as
     * separate series. Only the app range carries it, which is why the exporter keys off the range.
     */
    public static final int KEY_CLIENT_ID_OFFSET = BitUtil.SIZE_OF_INT;

    /** Length of the key buffer written by {@link #addCounter}: one int, the memberId. */
    public static final int KEY_LENGTH = BitUtil.SIZE_OF_INT;

    /** Length of the key buffer written by {@link #addAppCounter}: memberId then clientId. */
    public static final int APP_KEY_LENGTH = 2 * BitUtil.SIZE_OF_INT;

    /** Allocates an operator counter keyed on {@code memberId}, so a reader recovers the member without parsing the label. */
    public static Counter addCounter(final Aeron aeron, final int typeId, final String label, final int memberId) {
        final UnsafeBuffer keyBuffer = new UnsafeBuffer(new byte[KEY_LENGTH]);
        keyBuffer.putInt(KEY_MEMBER_ID_OFFSET, memberId);
        final byte[] labelBytes = label.getBytes(StandardCharsets.US_ASCII);
        final UnsafeBuffer labelBuffer = new UnsafeBuffer(labelBytes);
        return aeron.addCounter(typeId, keyBuffer, 0, KEY_LENGTH, labelBuffer, 0, labelBytes.length);
    }

    /**
     * Allocates an <b>app</b> counter keyed on {@code {memberId, clientId}}. The C++ twin is
     * {@code protocol/SeqeronCounters.hpp}'s {@code addAppCounter}; the key layout must match.
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
