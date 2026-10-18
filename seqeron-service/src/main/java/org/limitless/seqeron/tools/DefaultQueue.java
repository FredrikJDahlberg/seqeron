package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.QueueRequestEncoder;
import org.limitless.seqeron.sbe.replay.QueueResolvedDecoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingDecoder;

/**
 * The queue a tool reads when it is configured with none: the default queue (spec §3.1), which this node's
 * Replayer names in answer to a {@code QueueRequest} naming no source (R-6).
 */
final class DefaultQueue {
    /** The Replayer answered {@code ReplayPending}: no group is active yet. */
    static final int NOT_ACTIVE = 0;

    /** Nothing answered: no Replayer on this media driver. */
    static final int NO_REPLAYER = -1;

    private static final String CONTROL_CHANNEL = ReplayProtocol.IPC_CHANNEL + "|tether=false";
    private static final long RESEND_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(500);

    private DefaultQueue() {
    }

    /**
     * Asks until the Replayer names the default queue or {@code timeoutNs} passes.
     * @param aeron       a client of this node's media driver
     * @param timeoutNs   how long to ask; {@link Long#MAX_VALUE} asks for ever
     * @param untilActive wait through {@code ReplayPending} rather than return {@link #NOT_ACTIVE} on the first
     * @return the default queue's {@code groupId}, {@link #NOT_ACTIVE} or {@link #NO_REPLAYER}
     */
    static int resolve(final Aeron aeron, final long timeoutNs, final boolean untilActive) {
        final long clientId = aeron.nextCorrelationId();
        final long[] requestId = { 0 };
        final int[] answer = { NO_REPLAYER };
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final QueueResolvedDecoder resolved = new QueueResolvedDecoder();
        final ReplayPendingDecoder pending = new ReplayPendingDecoder();
        final FragmentHandler handler = (buffer, offset, length, h) -> {
            header.wrap(buffer, offset);
            if (header.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
                return;
            }
            final int body = offset + MessageHeaderDecoder.ENCODED_LENGTH;
            if (header.templateId() == QueueResolvedDecoder.TEMPLATE_ID) {
                resolved.wrap(buffer, body, header.blockLength(), header.version());
                if (resolved.clientId() == clientId && resolved.requestId() == requestId[0]) {
                    answer[0] = resolved.queueId();
                }
            } else if (header.templateId() == ReplayPendingDecoder.TEMPLATE_ID) {
                pending.wrap(buffer, body, header.blockLength(), header.version());
                if (pending.clientId() == clientId && pending.requestId() == requestId[0]) {
                    answer[0] = NOT_ACTIVE;
                }
            }
        };
        final QueueRequestEncoder request = new QueueRequestEncoder();
        final UnsafeBuffer requestBuffer =
            new UnsafeBuffer(new byte[MessageHeaderEncoder.ENCODED_LENGTH + QueueRequestEncoder.BLOCK_LENGTH]);
        final IdleStrategy idle = new YieldingIdleStrategy();
        try (Subscription control = aeron.addSubscription(CONTROL_CHANNEL, ReplayProtocol.CONTROL_STREAM_ID);
             Publication requests =
                 aeron.addPublication(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.REQUEST_STREAM_ID)) {
            final long start = System.nanoTime();
            long lastRequestNs = start - RESEND_INTERVAL_NS;
            while (answer[0] <= NOT_ACTIVE && (untilActive || answer[0] == NO_REPLAYER)) {
                final long now = System.nanoTime();
                if (now - start >= timeoutNs) {
                    break;
                }
                if (now - lastRequestNs >= RESEND_INTERVAL_NS && requests.isConnected()) {
                    request.wrapAndApplyHeader(requestBuffer, 0, new MessageHeaderEncoder())
                        .clientId(clientId)
                        .requestId(++requestId[0])
                        .sourceId(ReplayProtocol.NO_SOURCE_ID)
                        .gatewayName("");
                    requests.offer(requestBuffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + request.encodedLength());
                    lastRequestNs = now;
                }
                idle.idle(control.poll(handler, 10));
            }
        }
        return answer[0];
    }
}
