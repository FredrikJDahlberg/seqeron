package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.ApplicationRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.GroupsRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.UnsequencedEncoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemEncoder;
import org.limitless.seqeron.sequencer.Sequencer;

/**
 * A topology load through a real {@link Sequencer}, keeping what a member hands its replayer: copies of the
 * topology rows on the default queue, picked out by {@link QueueResolver#isTopologyRow} as SequencerService does.
 */
final class TopologyLoad {
    private static final long LOADER_SESSION = 0x70_0001L;
    private static final long TIMESTAMP = 1_000_000_000L;
    private static final int LOADER_SOURCE_ID = 99;

    private final MutableDirectBuffer ingress = new ExpandableArrayBuffer(1024);
    private final MutableDirectBuffer body = new ExpandableArrayBuffer(GroupsRegisteredEncoder.BLOCK_LENGTH);
    private final List<byte[]> rows = new ArrayList<>();
    private int defaultGroupId;

    private final Sequencer sequencer = new Sequencer(0, new Sequencer.FrameSink() {
        @Override
        public void activate(final int[] groupIds) {
            defaultGroupId = groupIds[0];
        }

        @Override
        public void publish(final int groupId, final DirectBuffer buffer, final int length) {
            if (groupId == defaultGroupId && QueueResolver.isTopologyRow(buffer, 0, length)) {
                final byte[] row = new byte[length];
                buffer.getBytes(0, row);
                rows.add(row);
            }
        }
    });

    /** The rows handed over so far, in log order. */
    List<byte[]> rows() {
        return rows;
    }

    Sequencer sequencer() {
        return sequencer;
    }

    /** A GroupsRegistered declaring each (groupId, name) pair given. */
    TopologyLoad groups(final Object... idsAndNames) {
        body.setMemory(0, GroupsRegisteredEncoder.BLOCK_LENGTH, (byte)0);
        final int slot = GroupsRegisteredEncoder.groupNamesLength() / FrameLayer.MAX_GROUPS;
        for (int i = 0; i < idsAndNames.length; i += 2) {
            body.putStringWithoutLengthAscii(((int)idsAndNames[i] - 1) * slot, (String)idsAndNames[i + 1]);
        }
        return submitSystem(SystemFrame.GROUPS_REGISTERED, GroupsRegisteredEncoder.BLOCK_LENGTH);
    }

    TopologyLoad gatewayRow(final int gatewayId, final int gatewaySourceId, final String name, final int groupId) {
        new GatewayRegisteredEncoder()
            .wrap(body, 0)
            .remaining(1) // not the last row, so no bootstrap designation follows
            .gatewayId(gatewayId)
            .gatewaySourceId(gatewaySourceId)
            .gatewayName(name)
            .preferenceRank((short)0)
            .snapshot((short)0)
            .groupId((short)groupId);
        return submitSystem(SystemFrame.GATEWAY_REGISTERED, GatewayRegisteredEncoder.BLOCK_LENGTH);
    }

    TopologyLoad applicationRow(final int sourceId, final int groupId) {
        new ApplicationRegisteredEncoder()
            .wrap(body, 0)
            .applicationSourceId(sourceId)
            .applicationName("app-" + sourceId)
            .snapshot((short)0)
            .groupId((short)groupId);
        return submitSystem(SystemFrame.APPLICATION_REGISTERED, ApplicationRegisteredEncoder.BLOCK_LENGTH);
    }

    /** An application frame addressed to {@code groupMask}. */
    TopologyLoad payload(final int groupMask) {
        final UnsequencedEncoder encoder = new UnsequencedEncoder();
        encoder.wrapAndApplyHeader(ingress, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(LOADER_SOURCE_ID).connectionId(1).sessionId(-1).payloadId(5).groupMask(groupMask);
        encoder.putPayload(new byte[1], 0, 1);
        return submit(MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    private TopologyLoad submitSystem(final int systemEventType, final int bodyLength) {
        final UnsequencedSystemEncoder encoder = new UnsequencedSystemEncoder();
        encoder.wrapAndApplyHeader(ingress, 0, new MessageHeaderEncoder());
        encoder.header()
            .sourceId(LOADER_SOURCE_ID)
            .connectionId(-1)
            .sessionId(-1)
            .systemEventType(systemEventType)
            .groupMask(0);
        encoder.putBody(body, 0, bodyLength);
        return submit(MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    private TopologyLoad submit(final int length) {
        assertTrue(sequencer.sequenceMessage(ingress, 0, length, LOADER_SESSION, TIMESTAMP),
                   "the sequencer refused it");
        return this;
    }
}
