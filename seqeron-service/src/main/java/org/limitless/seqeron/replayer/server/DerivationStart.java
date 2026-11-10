package org.limitless.seqeron.replayer.server;

import org.limitless.seqeron.sequencer.LogDeriver;

/**
 * Where a member's service started, handed to its Replayer so that a derivation of the queues from the Raft log
 * starts where the queues did (doc/disk-load-plan.md, step 2a).
 *
 * @param logPosition of the first log entry the service applies: 0, or the cluster snapshot's
 * @param state       the sequencer's there: nothing sequenced, or the snapshot's
 * @param restored    whether the service restored a snapshot, whose marker is then each queue's first frame
 * @param queueStarts where each queue's publication starts, slot {@code groupId - 1}
 * @param termLength  the term length the queues are published with
 * @param mtu         the MTU the queues are published with
 */
public record DerivationStart(long logPosition, LogDeriver.State state, boolean restored, long[] queueStarts,
                              int termLength, int mtu) { }
