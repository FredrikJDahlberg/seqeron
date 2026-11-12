package org.limitless.seqeron.sequencer;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.ClusterActionRequestDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.NewLeadershipTermEventDecoder;
import io.aeron.cluster.codecs.SessionCloseEventDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionOpenEventDecoder;
import io.aeron.cluster.codecs.TimerEventDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.sbe.sequencer.SequencerStateDecoder;

/**
 * Derives a member's queues from its Raft log: a {@link Sequencer} driven by the log's entries as {@link
 * SequencerService} drives one from the cluster's callbacks, so that its frames are the ones the member published
 * (doc/disk-load-plan.md, step 2a). It starts at the log's first entry or from a {@link State}, and keeps no queue
 * positions.
 *
 * <p>Free of the Aeron runtime: it reads the cluster's log codecs and nothing else. Single-threaded.
 */
public final class LogDeriver {
    /** {@code ClusterActionRequest.flags} of an action every member's service takes. */
    private static final int ACTION_FLAGS_DEFAULT = 0;

    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionMessage = new SessionMessageHeaderDecoder();
    private final TimerEventDecoder timer = new TimerEventDecoder();
    private final SessionOpenEventDecoder sessionOpen = new SessionOpenEventDecoder();
    private final SessionCloseEventDecoder sessionClose = new SessionCloseEventDecoder();
    private final ClusterActionRequestDecoder action = new ClusterActionRequestDecoder();
    private final NewLeadershipTermEventDecoder newTerm = new NewLeadershipTermEventDecoder();

    /**
     * The sessions the log has opened and not closed: the service container hands a message of any other no session.
     */
    private final LongHashSet openSessions = new LongHashSet();

    private final Sequencer sequencer;

    /**
     * @param memberId the member whose log this reads, for the log lines only
     * @param sink     receives every frame of every queue, in {@code clusterSeqNo} order
     */
    public LogDeriver(final int memberId, final Sequencer.FrameSink sink) {
        this.sequencer = new Sequencer(memberId, sink);
    }

    /**
     * What a deriver continues from between two log entries: a checkpoint of one, or a cluster snapshot.
     *
     * @param sequencer    the {@link Sequencer}'s snapshot records in order, or none for one that has sequenced nothing
     * @param openSessions the sessions the log has opened and not closed
     */
    public record State(List<byte[]> sequencer, long[] openSessions) { }

    /** This deriver's state, between two log entries. */
    public State state() {
        final long[] sessions = new long[openSessions.size()];
        int i = 0;
        for (final long sessionId : openSessions) {
            sessions[i++] = sessionId;
        }
        return stateOf(sequencer, sessions);
    }

    /** A sequencer's state between two log entries, as its snapshot records. */
    static State stateOf(final Sequencer sequencer, final long[] openSessions) {
        final List<byte[]> records = new ArrayList<>();
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(4096);
        for (int length; (length = sequencer.onSnapshot(buffer, records.size())) != 0;) {
            final byte[] record = new byte[length];
            buffer.getBytes(0, record);
            records.add(record);
        }
        return new State(records, openSessions);
    }

    /**
     * Continues from a state, in a deriver that has applied nothing.
     *
     * @param state        to continue from
     * @param reemitMarker whether to publish the state's snapshot marker first, as a member that starts from a cluster
     *                     snapshot does on each queue
     * @throws IllegalStateException on records the {@link Sequencer} cannot restore
     */
    public void restore(final State state, final boolean reemitMarker) {
        for (final long sessionId : state.openSessions()) {
            openSessions.add(sessionId);
        }
        final List<byte[]> records = state.sequencer();
        if (records.isEmpty()) {
            return;
        }
        final UnsafeBuffer record = new UnsafeBuffer(records.getFirst());
        final int formatVersion =
            new SequencerStateDecoder()
                .wrapAndApplyHeader(record, 0, new org.limitless.seqeron.sbe.sequencer.MessageHeaderDecoder())
                .formatVersion();
        for (int i = 0; i < records.size(); i++) {
            record.wrap(records.get(i))
            ;
            sequencer.onRestore(record, record.capacity(), i, formatVersion);
        }
        if (!sequencer.isRestored()) {
            throw new IllegalStateException("the Sequencer's state ended before its end record");
        }
        if (reemitMarker) {
            sequencer.reemitMarker();
        }
    }

    /** Hands over the topology rows a restored state holds, which the log no longer replays. */
    public void replayTopologyRows(final Consumer<byte[]> rows) {
        sequencer.replayTopologyRows(rows);
    }

    /** Where sequencing has reached. */
    public long clusterSeqNo() {
        return sequencer.clusterSeqNo();
    }

    /**
     * Applies one log entry, reassembled if it was fragmented. An entry of another schema or an unknown template is
     * skipped, as the service container skips it.
     *
     * @param buffer holding the entry
     * @param offset of the entry's {@code MessageHeader}
     * @param length of the entry
     */
    public void onLogEntry(final DirectBuffer buffer, final int offset, final int length) {
        header.wrap(buffer, offset);
        if (header.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
            return;
        }
        final int body = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int blockLength = header.blockLength();
        final int version = header.version();
        switch (header.templateId()) {
        case SessionMessageHeaderDecoder.TEMPLATE_ID -> {
            sessionMessage.wrap(buffer, body, blockLength, version);
            final long sessionId = sessionMessage.clusterSessionId();
            if (openSessions.contains(sessionId)) {
                sequencer.sequenceMessage(buffer, offset + AeronCluster.SESSION_HEADER_LENGTH,
                                          length - AeronCluster.SESSION_HEADER_LENGTH, sessionId,
                                          sessionMessage.timestamp());
            }
        }
        case TimerEventDecoder.TEMPLATE_ID -> {
            timer.wrap(buffer, body, blockLength, version);
            if (timer.correlationId() == SequencerService.HEARTBEAT_TIMER_CORRELATION_ID) {
                sequencer.clusterHeartbeat(timer.timestamp());
            }
        }
        case SessionOpenEventDecoder.TEMPLATE_ID -> {
            sessionOpen.wrap(buffer, body, blockLength, version);
            openSessions.add(sessionOpen.clusterSessionId());
        }
        case SessionCloseEventDecoder.TEMPLATE_ID -> {
            sessionClose.wrap(buffer, body, blockLength, version);
            if (openSessions.remove(sessionClose.clusterSessionId())) {
                sequencer.sessionClosed(sessionClose.clusterSessionId(), sessionClose.timestamp());
            }
        }
        case ClusterActionRequestDecoder.TEMPLATE_ID -> {
            action.wrap(buffer, body, blockLength, version);
            final int flags =
                action.flags() == ClusterActionRequestDecoder.flagsNullValue() ? ACTION_FLAGS_DEFAULT : action.flags();
            if (action.action() == ClusterAction.SNAPSHOT && flags == ACTION_FLAGS_DEFAULT &&
                sequencer.hasSnapshotPolicy()) {
                sequencer.takeSnapshot(action.timestamp());
            }
        }
        case NewLeadershipTermEventDecoder.TEMPLATE_ID -> {
            newTerm.wrap(buffer, body, blockLength, version);
            sequencer.leadershipChanged(newTerm.leadershipTermId(), newTerm.leaderMemberId(), newTerm.timestamp());
        }
        default -> {
        }
        }
    }
}
