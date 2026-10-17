package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.aeron.Aeron;
import io.aeron.test.Tests;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.collections.LongArrayList;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.sbe.probe.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.probe.ProbeMarkerDecoder;
import org.limitless.seqeron.sequencer.client.ClusterStreamSender;
import org.limitless.seqeron.sequencer.client.PendingSends;

/**
 * ClusterProbe confirm's producer, driven one duty cycle at a time: it sends seqNo 1..count through
 * {@link PendingSends} while following its member's queue, and resends what a leadership change lost.
 */
final class ConfirmProducer implements AutoCloseable {
    /** ClusterProbe confirm's: how long a run may go without sending, resending or seeing an own frame. */
    static final long DRAIN_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(15);

    private static final int CLIENT_ID = 9;
    private static final int NO_ID = -1;
    private static final byte[] NO_FILLER = new byte[0];

    private long count;
    private final long pacingNs;
    private final Aeron aeron;
    private final ClusterStreamSender sender = new ClusterStreamSender();
    private final ReplayerStreamReceiver receiver;
    private final PendingSends pending = new PendingSends(4096);
    private final ClusterProbe.MarkerEncoder marker = new ClusterProbe.MarkerEncoder();
    private final ProbeMarkerDecoder decoder = new ProbeMarkerDecoder();
    private final LongArrayList seqNos = new LongArrayList();
    private volatile int lastLeader = InProcessCluster.NO_MEMBER;
    private boolean caughtUp;
    private long next = 1;
    private long nextSendNs;
    private long lastProgressNs;
    private int resent;

    /** Attaches to member {@code memberId}'s driver, catches up on its queue, and connects over UDP ingress. */
    ConfirmProducer(final String aeronDir, final int memberId, final int count, final long pacingNs) {
        this.count = count;
        this.pacingNs = pacingNs;
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        receiver = new ReplayerStreamReceiver(CLIENT_ID, this::onSequenced, (leaderMemberId, leadershipTermId, gsn) -> {
            pending.onLeadershipChanged(leadershipTermId);
            lastLeader = leaderMemberId;
        }, () -> caughtUp = true);
        receiver.start(aeron, memberId);
        while (!caughtUp) {
            receiver.poll();
            Tests.yield();
        }
        sender.setIngressHold(pending);
        sender.connect(aeron, "aeron:udp?endpoint=" + PortLayout.DEFAULT_HOST + ":0",
                       PortLayout.ingressEndpoints(InProcessCluster.MEMBERS));
        lastProgressNs = System.nanoTime();
    }

    private void onSequenced(final SequencedEvent event) {
        pending.onSequenced(event);
        if (!event.isSystem() && event.payloadId() == ClusterProbe.PROBE_PAYLOAD_ID &&
            event.templateId() == ProbeMarkerDecoder.TEMPLATE_ID) {
            decoder.wrap(event.buffer(), event.payloadOffset() + MessageHeaderDecoder.ENCODED_LENGTH,
                         event.blockLength(), event.version());
            seqNos.addLong(decoder.seqNo());
        }
    }

    /** One duty cycle: poll, keep the session alive, resend what was lost, and send the next frame if it may. */
    void doWork() {
        final long now = System.nanoTime();
        final int seen = seqNos.size();
        receiver.poll();
        sender.pollEgress();
        sender.keepAlive();
        if (sender.isSessionLost() || pending.isFaulted()) {
            fail(pending.isFaulted() ? "PendingSends faulted" : "cluster session lost");
        }
        final int resentNow = pending.resendMissing(sender);
        resent += resentNow;
        if (next <= count && !pending.isHolding() && !pending.isFull() && now >= nextSendNs) {
            final int length = marker.encode(next, NO_ID, ClusterProbe.PROBE_SOURCE_ID, NO_FILLER);
            if (sender.send(marker.frame(), length)) {
                pending.track(marker.frame(), length, sender.clusterSessionId(), sender.leadershipTermId());
                next++;
                nextSendNs = now + pacingNs;
                lastProgressNs = now;
            }
        }
        if (resentNow > 0 || seqNos.size() > seen) {
            lastProgressNs = now;
        }
    }

    /** Sends nothing past what is already sent; what is held is still resent. */
    void stopSending() {
        count = next - 1;
    }

    long sent() {
        return next - 1;
    }

    /** Every frame sent and none still held. */
    boolean isDrained() {
        return next > count && pending.size() == 0;
    }

    /** Nothing sent, resent or seen for {@link #DRAIN_TIMEOUT_NS}. */
    boolean isStalled() {
        return System.nanoTime() - lastProgressNs > DRAIN_TIMEOUT_NS;
    }

    int held() {
        return pending.size();
    }

    int resent() {
        return resent;
    }

    /** The leader the newest {@code LeadershipChanged} named, or {@link InProcessCluster#NO_MEMBER}. */
    int lastLeader() {
        return lastLeader;
    }

    /** The queue showed seqNo 1..count, each once, in order. */
    void assertExactlyOnceInOrder() {
        final LongArrayList expected = new LongArrayList();
        for (long seqNo = 1; seqNo <= count; seqNo++) {
            expected.addLong(seqNo);
        }
        assertEquals(expected, seqNos,
                     "the queue showed " + seqNos.size() + " of " + count + " frame(s), not 1.." + count +
                         " in order; resent " + resent);
    }

    @Override
    public void close() {
        CloseHelper.closeAll(receiver::close, sender, aeron);
    }
}
