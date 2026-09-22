/**
 * Producing: the cluster session and the encode-and-offer above it.
 *
 * <p>{@link org.limitless.seqeron.sequencer.client.ClusterStreamSender} is the session — IPC ingress on the
 * co-located member, UDP when that member is not leading, spinning through back-pressure and elections
 * rather than dropping a frame. {@link org.limitless.seqeron.sequencer.client.IngressPublisher} wraps a body
 * in its envelope and offers it, returning {@link org.limitless.seqeron.protocol.Publish}.
 * {@link org.limitless.seqeron.sequencer.client.PendingSends} is what makes a send a frame sequenced.
 *
 * <p>This is what the façades in {@code org.limitless.seqeron.app} are assembled from. A producer that takes
 * one never sees this package; reach for it directly only to assemble a duty cycle of your own.
 * {@link org.limitless.seqeron.sequencer.client.IngressTracker} and
 * {@link org.limitless.seqeron.sequencer.client.IngressSender} are the seams {@code PendingSends} and
 * {@code ClusterStreamSender} fill — implement them only to replace those.
 *
 * <p>A send that succeeds is not a frame sequenced: nothing confirms ingress on egress, and a leader
 * failover silently loses whatever the old leader had not committed. {@code PendingSends} is what closes
 * that gap (spec §16 A-4, A-5).
 */
package org.limitless.seqeron.sequencer.client;
