package org.limitless.seqeron.sequencer;

import org.agrona.DirectBuffer;

/**
 * What {@link IngressPublisher} needs of a cluster session: place this frame, and say which session it
 * belongs to. {@link ClusterStreamSender} is the implementation.
 *
 * <p>The C++ twin's {@code publishPayload}/{@code publishSystem} take the {@code ClusterStreamSender}
 * itself, because that class carries its own {@code IngressTransport}/{@code EgressTransport} seam and a
 * test drives it with fakes. The Java sender's transport is {@code AeronCluster}, which cannot be faked
 * without an Aeron runtime the Java suite deliberately has none of — so the seam sits one level up, here.
 */
public interface IngressSender {
    /**
     * Offers one pre-encoded frame to cluster ingress.
     * @param frame  the frame, from offset 0
     * @param length its length in bytes
     * @return whether it was placed
     */
    boolean send(DirectBuffer frame, int length);

    /** This process's cluster session id, or -1 with no session. Stamped into a frame's advisory field. */
    long clusterSessionId();
}
