package org.limitless.seqeron.sequencer;

import org.agrona.DirectBuffer;

/**
 * What {@link IngressPublisher} needs of a cluster session: place this frame, and say which session it
 * belongs to. The seam the Java suite drives without an Aeron runtime; {@link ClusterStreamSender}
 * implements it.
 */
public interface IngressSender {
    /**
     * Offers one pre-encoded frame to cluster ingress.
     * @param frame  the frame, from offset 0
     * @param length its length in bytes
     * @return whether it was placed; false with a session still open means a new leader arrived while an
     *     {@link IngressHold} held, and the frame goes again once it releases
     */
    boolean send(DirectBuffer frame, int length);

    /** This process's cluster session id, or -1 with no session. Stamped into a frame's advisory field. */
    long clusterSessionId();

    /**
     * The leadership term ingress is stamped with, or -1 with no session. Read straight after a
     * {@link #send} that returned true, it is the term that frame carried: the cluster drops a frame
     * stamped with any term but its own.
     */
    long leadershipTermId();
}
