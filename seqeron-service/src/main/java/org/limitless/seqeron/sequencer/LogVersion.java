package org.limitless.seqeron.sequencer;

import io.aeron.cluster.VersionValidator;
import org.agrona.SemanticVersion;

/**
 * The version a node's Raft log and snapshots carry (doc/snapshot.md §10.2), as Aeron's {@code appVersion}. Aeron
 * compares the major when it loads a snapshot and when it replays a leadership term from the log, so a node started on
 * a log it is not compatible with stops with "incompatible version" rather than sequence another history.
 *
 * <p>The major is {@link #LOG_VERSION}, raised by every change that spec V-3 says needs a purged archive, and by a
 * snapshot format the next release cannot read. Schema 210's own {@code version} stays 0 (spec §8), so it cannot
 * serve. A log written before there was a version carries Aeron's default major, 0, and is accepted until the first
 * raise.
 */
public final class LogVersion {
    /** The major version of the log this build writes. */
    public static final int LOG_VERSION = 1;

    /** What the consensus module and the service container are configured with. */
    public static final int APP_VERSION = SemanticVersion.compose(LOG_VERSION, 0, 0);

    /** Accepts the logs and snapshots of this build's major, and of Aeron's default major, 0. */
    public static final VersionValidator VALIDATOR =
        (contextAppVersion, appVersionUnderTest) -> SemanticVersion.major(appVersionUnderTest) == LOG_VERSION
        || SemanticVersion.major(appVersionUnderTest) == 0;

    private LogVersion() {
    }
}
