package org.limitless.seqeron.protocol;

import static io.aeron.Aeron.NULL_VALUE;

/**
 * The replay protocol's addresses, shared by the Replayer and its co-located clients. The C++ twin is
 * {@code protocol/ReplayProtocol.hpp}.
 */
public final class ReplayProtocol {
    /** The whole protocol is node-local: a replica and its Replayer share one media driver. */
    public static final String IPC_CHANNEL = "aeron:ipc?alias=seqeron-replay";

    /** ReplayerService → apps: on-demand archive replays (one Aeron session per in-flight replay). */
    public static final int REPLAY_STREAM_ID = 201;

    /** Apps → ReplayerService: {@code ReplayRequest} / {@code QueueRequest}. */
    public static final int REQUEST_STREAM_ID = 202;

    /** ReplayerService → apps: {@code Replaying} / {@code ReplayPending} / {@code QueueResolved}. */
    public static final int CONTROL_STREAM_ID = 203;

    /** {@code ReplayRequest.fromPosition} asking for the active recording from its start. */
    public static final long FROM_START = NULL_VALUE;

    /** Answer to a resume request the Replayer refuses, or one that needs no replay at all. */
    public static final long NO_REPLAY_NEEDED = NULL_VALUE;

    /** {@code QueueRequest.sourceId} naming no application. */
    public static final int NO_SOURCE_ID = -1;

    private ReplayProtocol() {
    }
}
