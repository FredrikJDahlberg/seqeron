package org.limitless.seqeron.replayer.server;

import java.util.Queue;

/**
 * What a member's Replayer is given to track its cluster snapshots (doc/snapshot.md §10.8): the cuts the service hands
 * over from its own thread, the member's record of which snapshots may be restarted from, and how long an application
 * has to join a round.
 *
 * @param cuts         each cut, as the service takes it
 * @param log          the member's {@code restart-safe.log}
 * @param joinWindowMs how long after a cut an application may join its round
 */
public record CutIntake(Queue<CutHandover> cuts, RestartSafeLog log, long joinWindowMs) { }
