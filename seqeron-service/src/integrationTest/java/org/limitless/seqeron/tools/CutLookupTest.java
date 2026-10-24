package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingDecoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingDecoder;

/**
 * A member's Replayer finds a snapshot cut in its own archive by reading the recording, as a restoring client asks
 * (doc/snapshot.md §10.8): the request names the cut by its {@code clusterSeqNo} and its position is only a hint.
 */
@ExtendWith(InterruptingTestCallback.class)
class CutLookupTest {
    private static final int CLIENT_ID = 11;
    private static final long TIMEOUT_MS = 20_000;
    private static final int FRAMES = 5;

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir
    Path baseDir;

    private InProcessCluster cluster;

    /** One frame read off the live queue: where it starts and its {@code clusterSeqNo}. */
    private record Seen(long position, long clusterSeqNo) { }

    @BeforeEach
    void startCluster() {
        // The archive logs the hint that is no frame boundary, which this test sends on purpose.
        watcher.ignoreErrorsMatching(s -> s.contains("does not point to a valid frame"));
        watcher.dataCollector().add(baseDir);
        cluster = new InProcessCluster(baseDir);
        cluster.startAll();
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @InterruptAfter(120)
    void aCutIsFoundWhereItsFrameIsWhateverTheHintAndRefusedOrHeldOutsideTheRecording() {
        final int member = cluster.awaitLeader();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
             Subscription queue = aeron.addSubscription(ReplayerStreamReceiver.QUEUE_CONSUMER_CHANNEL,
                                                        FrameLayer.queueStreamId(InProcessCluster.GROUP_ID));
             Subscription control = aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|tether=false",
                                                          ReplayProtocol.CONTROL_STREAM_ID);
             Publication requests =
                 aeron.addPublication(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.REQUEST_STREAM_ID)) {
            final Requester requester = new Requester(aeron.nextCorrelationId(), control, requests);
            final List<Seen> seen = readLive(queue);
            final Seen cut = seen.get(2);

            for (final long hint : new long[] {cut.position(), 0, cut.position() + 32, seen.get(4).position()}) {
                final ReplayingDecoder reply = requester.ask(hint, cut.clusterSeqNo());
                assertTrue(reply.replaySessionId() >= 0, "a replay for the cut with hint " + hint);
                assertEquals(cut, firstFrameOfReplay(aeron, reply.replaySessionId()), "hint " + hint);
            }

            assertEquals(ReplayProtocol.NO_REPLAY_NEEDED, requester.ask(0, 0).replaySessionId(),
                         "a cut before the recording's first frame is refused");
            assertTrue(requester.isHeld(0, Long.MAX_VALUE / 2), "a cut the recording has not reached is held");
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
        assertFalse(cluster.terminated(), "a member terminated");
    }

    /** Reads frames off the live queue until {@link #FRAMES} have been seen: the heartbeat supplies them. */
    private static List<Seen> readLive(final Subscription queue) {
        final List<Seen> seen = new ArrayList<>();
        final SequencedFrameDecoder frame = new SequencedFrameDecoder();
        final FragmentAssembler handler = new FragmentAssembler((buffer, offset, length, header) -> {
            if (frame.wrap(buffer, offset, length)) {
                seen.add(new Seen(position(header), frame.clusterSeqNo()));
            }
        });
        final long deadlineMs = System.currentTimeMillis() + TIMEOUT_MS;
        while (seen.size() < FRAMES) {
            assertTrue(System.currentTimeMillis() < deadlineMs, "the queue carried " + seen.size() + " frame(s)");
            if (queue.poll(handler, 1) == 0) {
                Tests.yield();
            }
        }
        return seen;
    }

    /** The first frame a replay delivers. */
    private static Seen firstFrameOfReplay(final Aeron aeron, final long replaySessionId) {
        final List<Seen> seen = new ArrayList<>();
        final SequencedFrameDecoder frame = new SequencedFrameDecoder();
        final String channel = ReplayProtocol.IPC_CHANNEL + "|session-id=" + (int)replaySessionId;
        try (Subscription replay = aeron.addSubscription(channel, ReplayProtocol.REPLAY_STREAM_ID)) {
            final FragmentAssembler handler = new FragmentAssembler((buffer, offset, length, header) -> {
                if (seen.isEmpty() && frame.wrap(buffer, offset, length)) {
                    seen.add(new Seen(position(header), frame.clusterSeqNo()));
                }
            });
            final long deadlineMs = System.currentTimeMillis() + TIMEOUT_MS;
            while (seen.isEmpty()) {
                assertTrue(System.currentTimeMillis() < deadlineMs, "the replay delivered nothing");
                if (replay.poll(handler, 1) == 0) {
                    Tests.yield();
                }
            }
        }
        return seen.getFirst();
    }

    private static long position(final io.aeron.logbuffer.Header header) {
        return LogBufferDescriptor.computePosition(header.termId(), header.termOffset(), header.positionBitsToShift(),
                                                   header.initialTermId());
    }

    /** A client of the Replayer: sends {@code ReplayRequest}s naming a cut and reads the answers. */
    private static final class Requester {
        private final long clientId;
        private final Subscription control;
        private final Publication requests;
        private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(64);
        private final ReplayRequestEncoder encoder = new ReplayRequestEncoder();
        private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final ReplayingDecoder replaying = new ReplayingDecoder();
        private final ReplayPendingDecoder pending = new ReplayPendingDecoder();
        private long requestId;
        private int templateId;

        Requester(final long clientId, final Subscription control, final Publication requests) {
            this.clientId = clientId;
            this.control = control;
            this.requests = requests;
        }

        /** Asks until the Replayer answers with a replay, or refuses; a held request is asked again. */
        ReplayingDecoder ask(final long hint, final long cutClusterSeqNo) {
            final long deadlineMs = System.currentTimeMillis() + TIMEOUT_MS;
            while (System.currentTimeMillis() < deadlineMs) {
                if (send(hint, cutClusterSeqNo) == ReplayingDecoder.TEMPLATE_ID) {
                    return replaying;
                }
            }
            throw new AssertionError("no replay for cut " + cutClusterSeqNo + " with hint " + hint);
        }

        /** Whether the Replayer holds the request, every time it is asked. */
        boolean isHeld(final long hint, final long cutClusterSeqNo) {
            for (int i = 0; i < 3; i++) {
                if (send(hint, cutClusterSeqNo) != ReplayPendingDecoder.TEMPLATE_ID) {
                    return false;
                }
            }
            return true;
        }

        /** Sends one request and returns the template of the answer to it, after the Replayer answers. */
        private int send(final long hint, final long cutClusterSeqNo) {
            encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(clientId)
                .requestId(++requestId)
                .fromPosition(hint)
                .queueId(InProcessCluster.GROUP_ID)
                .cutClusterSeqNo(cutClusterSeqNo);
            final int length = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
            while (requests.offer(buffer, 0, length) < 0) {
                Tests.yield();
            }
            templateId = 0;
            final long deadlineMs = System.currentTimeMillis() + TIMEOUT_MS;
            while (templateId == 0 && System.currentTimeMillis() < deadlineMs) {
                if (control.poll(this::onControl, 1) == 0) {
                    Tests.yield();
                }
            }
            return templateId;
        }

        private void onControl(final org.agrona.DirectBuffer data, final int offset, final int length,
                               final io.aeron.logbuffer.Header unused) {
            header.wrap(data, offset);
            final int body = offset + MessageHeaderDecoder.ENCODED_LENGTH;
            if (header.templateId() == ReplayingDecoder.TEMPLATE_ID) {
                replaying.wrap(data, body, header.blockLength(), header.version());
                if (replaying.clientId() == clientId && replaying.requestId() == requestId) {
                    templateId = ReplayingDecoder.TEMPLATE_ID;
                }
            } else if (header.templateId() == ReplayPendingDecoder.TEMPLATE_ID) {
                pending.wrap(data, body, header.blockLength(), header.version());
                if (pending.clientId() == clientId && pending.requestId() == requestId) {
                    templateId = ReplayPendingDecoder.TEMPLATE_ID;
                }
            }
        }
    }
}
