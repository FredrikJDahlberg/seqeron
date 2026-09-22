/**
 * Consuming the ordered stream: the client side of the replay protocol.
 *
 * <p>{@link org.limitless.seqeron.replayer.client.ReplayerStreamReceiver} is the entry point. It replays
 * this node's history through the co-located Replayer, switches to the live tap once caught up, heals gaps
 * by itself, and delivers every frame once in {@code globalSeqNo} order to the three handlers
 * ({@link org.limitless.seqeron.replayer.client.SequencedHandler},
 * {@link org.limitless.seqeron.replayer.client.LeadershipHandler},
 * {@link org.limitless.seqeron.replayer.client.CaughtUpHandler}), each frame as a
 * {@link org.limitless.seqeron.replayer.client.SequencedEvent} valid only during the call.
 *
 * <p>A producer that takes a façade from {@code org.limitless.seqeron.app} does not construct one: the
 * façade owns it and hands out {@code Payload}s instead. Reach for this package to follow the stream
 * without a façade — a read-only consumer, or a duty cycle of your own.
 *
 * <p>The server side is {@code org.limitless.seqeron.replayer.server}, in the service tier.
 */
package org.limitless.seqeron.replayer.client;
