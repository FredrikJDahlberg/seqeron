/**
 * The client application interface.
 *
 * <p>Take {@link org.limitless.seqeron.app.Gateway} (one instance of an elected active/standby pair) or
 * {@link org.limitless.seqeron.app.Application} (one replica per node, publishing only while its
 * own node leads), or {@link org.limitless.seqeron.app.Consumer} to read a queue and publish nothing. Each
 * assembles the duty cycle — the cluster session, the queue, confirmed ingress, the fences, the election — so a consumer writes its edge and its payloads and nothing of the frame layer
 * or of seqeron's system vocabulary appears in its code. A listener sees
 * {@link org.limitless.seqeron.app.Payload} and {@link org.limitless.seqeron.app.ClusterError}; a publish returns
 * {@link org.limitless.seqeron.protocol.Publish}, the one type from outside this package a façade names.
 *
 * <p>One block the façades are assembled from is offered on its own:
 * {@link org.limitless.seqeron.app.OutstandingWork}, which is about the application's own request/reply
 * work rather than seqeron's plumbing. The rest — the election, the leader gate and the two stall fences —
 * are package-private, being decisions the façades make and nothing else does; confirmed ingress is
 * {@code sequencer.client.PendingSends}, public for seqeron's own tools but not client API.
 *
 * <p>A source that takes part in snapshot rounds gives its façade a
 * {@link org.limitless.seqeron.app.SnapshotListener}, which the façade pulls the records of its state from, and
 * hands them back to on a restore.
 *
 * <p>The policy knobs a producer rarely changes are one {@link org.limitless.seqeron.app.Tuning}, and a reply
 * names the request it answers by a {@link org.limitless.seqeron.app.ReplyAddress}, which unlike the
 * {@link org.limitless.seqeron.app.Payload} outlives the callback.
 *
 * <p>{@code doc/client-api.md} is this surface in full.
 */
package org.limitless.seqeron.app;
