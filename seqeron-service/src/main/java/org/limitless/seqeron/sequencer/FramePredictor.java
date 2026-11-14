package org.limitless.seqeron.sequencer;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.codecs.TimerEventDecoder;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedEncoder;
import org.limitless.seqeron.sbe.frame.SequencedHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemEncoder;
import org.limitless.seqeron.sbe.frame.UnsequencedDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedHeaderDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemDecoder;

/**
 * The frames a {@link Sequencer} publishes for the log entries whose frames follow from the entry and three counts
 * alone: an application message, sequenced onto the queue of each group its mask names, and on every queue a system
 * message it admits with nothing synthesized behind it and the heartbeat timer's {@code ClusterHeartbeat}.
 *
 * <p>It validates nothing and holds none of the sequencer's state, so its frames are the sequencer's only where
 * something has compared the two: {@code DerivingReplayer} does for each entry, and replays with this over the
 * stretches of log where they agreed. For any other entry it publishes nothing.
 *
 * <p>Free of the Aeron runtime. Single-threaded.
 */
public final class FramePredictor {
    private static final int QUEUE_BODY_PREFIX_OFFSET =
        MessageHeaderEncoder.ENCODED_LENGTH + SequencedHeaderEncoder.ENCODED_LENGTH;

    private final io.aeron.cluster.codecs.MessageHeaderDecoder entryHeader =
        new io.aeron.cluster.codecs.MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionMessage = new SessionMessageHeaderDecoder();
    private final TimerEventDecoder timer = new TimerEventDecoder();
    private final MessageHeaderDecoder ingressHeader = new MessageHeaderDecoder();
    private final UnsequencedHeaderDecoder ingressFrameHeader = new UnsequencedHeaderDecoder();
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final SequencedHeaderEncoder queueHeader = new SequencedHeaderEncoder();
    private final ClusterHeartbeatEncoder heartbeat = new ClusterHeartbeatEncoder();
    private final MutableDirectBuffer frame = new ExpandableArrayBuffer(4096);
    private final long[] groupSeqNos = new long[FrameLayer.MAX_GROUPS];
    private long clusterSeqNo;
    private int activeGroupMask;

    /**
     * Continues from where a sequencer stood between two log entries.
     *
     * @param clusterSeqNo    of the last frame it sequenced
     * @param activeGroupMask the groups whose queues its own frames reach
     * @param groupSeqNos     of the last frame on each queue, slot {@code groupId - 1}
     */
    public void restore(final long clusterSeqNo, final int activeGroupMask, final long[] groupSeqNos) {
        this.clusterSeqNo = clusterSeqNo;
        this.activeGroupMask = activeGroupMask;
        System.arraycopy(groupSeqNos, 0, this.groupSeqNos, 0, this.groupSeqNos.length);
    }

    /**
     * Counts one log entry's frames, and publishes the ones on the wanted queues.
     *
     * @param buffer     holding the entry, reassembled if it was fragmented
     * @param offset     of the entry's {@code MessageHeader}
     * @param length     of the entry
     * @param wantedMask the groups whose frames {@code sink} takes
     * @param sink       takes each frame as {@link Sequencer.FrameSink#publish} describes it
     */
    public void onLogEntry(final DirectBuffer buffer, final int offset, final int length, final int wantedMask,
                           final Sequencer.FrameSink sink) {
        entryHeader.wrap(buffer, offset);
        if (entryHeader.schemaId() != io.aeron.cluster.codecs.MessageHeaderDecoder.SCHEMA_ID) {
            return;
        }
        final int body = offset + io.aeron.cluster.codecs.MessageHeaderDecoder.ENCODED_LENGTH;
        final int templateId = entryHeader.templateId();
        if (templateId == SessionMessageHeaderDecoder.TEMPLATE_ID) {
            sessionMessage.wrap(buffer, body, entryHeader.blockLength(), entryHeader.version());
            onMessage(buffer, offset + AeronCluster.SESSION_HEADER_LENGTH, length - AeronCluster.SESSION_HEADER_LENGTH,
                      sessionMessage.clusterSessionId(), sessionMessage.timestamp(), wantedMask, sink);
        } else if (templateId == TimerEventDecoder.TEMPLATE_ID) {
            timer.wrap(buffer, body, entryHeader.blockLength(), entryHeader.version());
            if (timer.correlationId() == SequencerService.HEARTBEAT_TIMER_CORRELATION_ID) {
                onHeartbeat(timer.timestamp(), wantedMask, sink);
            }
        }
    }

    /** An application or a system message, as {@link Sequencer#sequenceMessage} encodes one it admits. */
    private void onMessage(final DirectBuffer buffer, final int offset, final int length, final long sessionId,
                           final long timestamp, final int wantedMask, final Sequencer.FrameSink sink) {
        if (length < FrameLayer.MIN_INGRESS_LENGTH || length > FrameLayer.MAX_INGRESS_LENGTH) {
            return;
        }
        ingressHeader.wrap(buffer, offset);
        final boolean system = ingressHeader.templateId() == UnsequencedSystemDecoder.TEMPLATE_ID;
        if (ingressHeader.schemaId() != MessageHeaderDecoder.SCHEMA_ID ||
            (!system && ingressHeader.templateId() != UnsequencedDecoder.TEMPLATE_ID)) {
            return;
        }
        final int headerOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int groupMask = system ? activeGroupMask : ingressFrameHeader.wrap(buffer, headerOffset).groupMask();
        final long clusterSeq = ++clusterSeqNo;
        final int frameLength =
            length - UnsequencedHeaderDecoder.ENCODED_LENGTH + SequencedHeaderEncoder.ENCODED_LENGTH;
        if ((groupMask & wantedMask) != 0) {
            header.wrap(frame, 0)
                .blockLength(system ? SequencedSystemEncoder.BLOCK_LENGTH : SequencedEncoder.BLOCK_LENGTH)
                .templateId(system ? SequencedSystemEncoder.TEMPLATE_ID : SequencedEncoder.TEMPLATE_ID)
                .schemaId(MessageHeaderEncoder.SCHEMA_ID)
                .version(MessageHeaderEncoder.SCHEMA_VERSION);
            frame.putBytes(MessageHeaderEncoder.ENCODED_LENGTH, buffer, headerOffset,
                           UnsequencedHeaderDecoder.ENCODED_LENGTH);
            queueHeader.wrap(frame, MessageHeaderEncoder.ENCODED_LENGTH)
                .sessionId(sessionId)
                .clusterSeqNo(clusterSeq)
                .timestamp(timestamp);
            final int prefixOffset = headerOffset + UnsequencedHeaderDecoder.ENCODED_LENGTH;
            frame.putBytes(QUEUE_BODY_PREFIX_OFFSET, buffer, prefixOffset, offset + length - prefixOffset);
        }
        publish(frameLength, groupMask, wantedMask, sink);
    }

    /** The heartbeat timer's frame, as {@link Sequencer#clusterHeartbeat} encodes it. */
    private void onHeartbeat(final long timestamp, final int wantedMask, final Sequencer.FrameSink sink) {
        final long clusterSeq = ++clusterSeqNo;
        heartbeat.wrapAndApplyHeader(frame, 0, header);
        heartbeat.header()
            .sourceId(Sequencer.NO_SOURCE_ID)
            .connectionId(Sequencer.NO_SOURCE_ID)
            .sessionId(Sequencer.NO_SOURCE_ID)
            .systemEventType(SystemFrame.CLUSTER_HEARTBEAT)
            .groupMask(0)
            .clusterSeqNo(clusterSeq)
            .timestamp(timestamp);
        publish(MessageHeaderEncoder.ENCODED_LENGTH + heartbeat.encodedLength(), activeGroupMask, wantedMask, sink);
    }

    /** Stamps each queue's next {@code groupSeqNo}, as {@code Sequencer.publish} does, in ascending group. */
    private void publish(final int length, final int groupMask, final int wantedMask, final Sequencer.FrameSink sink) {
        for (int groupId = 1; groupId <= FrameLayer.MAX_GROUPS; groupId++) {
            final int bit = 1 << (groupId - 1);
            if ((groupMask & bit) != 0) {
                final long groupSeqNo = ++groupSeqNos[groupId - 1];
                if ((wantedMask & bit) != 0) {
                    queueHeader.wrap(frame, MessageHeaderEncoder.ENCODED_LENGTH).groupSeqNo(groupSeqNo);
                    sink.publish(groupId, frame, length);
                }
            }
        }
    }
}
