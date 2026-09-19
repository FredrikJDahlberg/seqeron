package org.limitless.seqeron.protocol;

import static io.aeron.Aeron.NULL_VALUE;

/**
 * The replay protocol's addresses, shared by the Replayer and its co-located clients. The C++ twin is
 * {@code protocol/ReplayProtocol.hpp}.
 */
public final class ReplayProtocol {
    public static final String IPC_CHANNEL = "aeron:ipc";

    /** ReplayerService → apps: on-demand archive replays (one Aeron session per in-flight replay). */
    public static final int REPLAY_STREAM_ID = 201;

    /** Apps → ReplayerService: {@code ReplayRequest}. */
    public static final int REQUEST_STREAM_ID = 202;

    /** ReplayerService → apps: {@code Replaying} / {@code ReplayPending}. */
    public static final int CONTROL_STREAM_ID = 203;

    /** Answer to a resume request the Replayer refuses, or one that needs no replay at all. */
    public static final long NO_REPLAY_NEEDED = NULL_VALUE;

    private ReplayProtocol() {
    }
}
