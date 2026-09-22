/**
 * The client application interface.
 *
 * <p>Take {@link org.limitless.seqeron.app.Gateway} (one instance of an elected active/standby pair) or
 * {@link org.limitless.seqeron.app.ColocatedApplication} (one replica per node, publishing only while its
 * own node leads). Either one assembles the duty cycle — the cluster session, the tap, confirmed ingress,
 * the fences, the election — so a consumer writes its edge and its payloads and nothing of the frame layer
 * or of seqeron's system vocabulary appears in its code. A listener sees
 * {@link org.limitless.seqeron.app.Payload} and {@link org.limitless.seqeron.app.ClusterError}; a publish returns
 * {@link org.limitless.seqeron.protocol.Publish}, the one type from outside this package a façade names.
 * {@link org.limitless.seqeron.app.TapLagMonitor} is reached through {@code tapLag()}.
 *
 * <p>One block the façades are assembled from is offered on its own:
 * {@link org.limitless.seqeron.app.OutstandingWork}, which is about the application's own request/reply
 * work rather than seqeron's plumbing. The rest — the election, the leader gate and the two stall fences —
 * are package-private, being decisions the façades make and nothing else does; confirmed ingress, which a
 * duty cycle of your own does need, is {@code sequencer.client.PendingSends}.
 *
 * <p>{@code doc/client-api.md} is this surface in full.
 */
package org.limitless.seqeron.app;
