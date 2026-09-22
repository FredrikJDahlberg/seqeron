/**
 * The wire contract in code, shared by the client tier and the service tier.
 *
 * <p>Anything both a client and the cluster must agree on lives here rather than in a service-tier class:
 * the tap's identity and the size limits ({@link org.limitless.seqeron.protocol.FrameLayer}), the
 * {@code systemEventType} vocabulary and the envelope both sides encode
 * ({@link org.limitless.seqeron.protocol.SystemFrame}), the envelope stripped off a tap frame
 * ({@link org.limitless.seqeron.protocol.SequencedFrameDecoder}), the cluster's port block
 * ({@link org.limitless.seqeron.protocol.PortLayout}), the replay protocol's addresses
 * ({@link org.limitless.seqeron.protocol.ReplayProtocol}), the counter type ids
 * ({@link org.limitless.seqeron.protocol.SeqeronCounters}) and what a publish did
 * ({@link org.limitless.seqeron.protocol.Publish}).
 *
 * <p>API, and it changes only when the protocol does. {@code doc/seqeron-protocol-spec.md} is normative;
 * each class here carries the C++ twin it must stay in step with.
 */
package org.limitless.seqeron.protocol;
