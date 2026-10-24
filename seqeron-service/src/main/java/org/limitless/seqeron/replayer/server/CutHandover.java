package org.limitless.seqeron.replayer.server;

/**
 * A cluster snapshot as the service takes it, handed to the member's Replayer (doc/snapshot.md §10.8).
 *
 * @param round           the round's number
 * @param clusterSeqNo    its cut: the {@code clusterSeqNo} of the marker
 * @param logPosition     the snapshot's position in the Raft log
 * @param markerPositions where the marker started in each queue's recording, slot {@code groupId - 1}, or -1 for a
 *                        group that is not active
 */
public record CutHandover(long round, long clusterSeqNo, long logPosition, long[] markerPositions) { }
