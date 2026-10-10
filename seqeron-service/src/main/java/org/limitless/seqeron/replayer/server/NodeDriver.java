package org.limitless.seqeron.replayer.server;

import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.agrona.SystemUtil;
import org.agrona.concurrent.IdleStrategy;
import org.limitless.seqeron.util.Logger;

/**
 * What a seqeron media driver is, a member's or a gateway host's: the driver's settings, the control-response
 * streams of the archive clients sharing it, and the exit statuses of the process that runs it.
 */
public final class NodeDriver {
    /** Exit status of a process that can no longer record its tap or serve replay; restart it. */
    public static final int EXIT_FATAL = 70;

    /**
     * Exit status when shutdown gave up waiting for a wedged duty-cycle thread, leaving the archive and Aeron
     * client unclosed: not an orderly stop, and not a dead thread either.
     */
    public static final int EXIT_SHUTDOWN_TIMEOUT = 71;

    // Control-response streams of the archive clients on one driver. None may be 101, the cluster's IPC
    // ingress stream, or archive replies misdecode as ingress. Clients may share one: the archive demuxes them
    // by controlSessionId.

    /** A member's consensus module, service container and {@code SequencerService}. */
    public static final int SEQUENCER_ARCHIVE_RESPONSE_STREAM_ID = 121;

    /** {@link ReplayerServer}'s. */
    static final int REPLAYER_ARCHIVE_RESPONSE_STREAM_ID = 120;

    /** {@link AeronTapRelay}'s session with a member's archive. */
    static final int RELAY_ARCHIVE_RESPONSE_STREAM_ID = 122;

    /** IPC term length when {@code aeron.ipc.term.buffer.length} is not set, rather than Aeron's 64 MiB. */
    private static final int DEFAULT_IPC_TERM_BUFFER_LENGTH = 16 * 1024 * 1024;

    /** Driver timer interval when {@code aeron.timer.interval} is not set; Aeron checks untethered timeouts on it. */
    static final long DEFAULT_TIMER_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(10);

    /**
     * Untethered window-limit and linger timeouts when their Aeron properties are not set, rather than Aeron's 5s
     * each. Until evicted, a stalled subscriber holds the tap's window, and the sequencer and a gateway host's
     * relay each terminate after 1s of back-pressure with no recording progress.
     */
    static final long DEFAULT_UNTETHERED_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(100);

    private NodeDriver() {
    }

    /**
     * A seqeron media driver's context: dedicated threads on {@code idleStrategies}, a directory deleted on start,
     * and the IPC term length and untethered timing of doc/ops.md ("Term lengths", "Untethered subscribers")
     * where their Aeron properties are not set.
     *
     * @param aeronDir       the driver's directory
     * @param idleStrategies the idle strategy of each driver thread
     */
    public static MediaDriver.Context context(final String aeronDir, final Supplier<IdleStrategy> idleStrategies) {
        final MediaDriver.Context ctx = new MediaDriver.Context()
                                            .aeronDirectoryName(aeronDir)
                                            .threadingMode(ThreadingMode.DEDICATED)
                                            .conductorIdleStrategy(idleStrategies.get())
                                            .senderIdleStrategy(idleStrategies.get())
                                            .receiverIdleStrategy(idleStrategies.get())
                                            .dirDeleteOnStart(true);
        if (System.getProperty(Configuration.IPC_TERM_BUFFER_LENGTH_PROP_NAME) == null) {
            ctx.ipcTermBufferLength(DEFAULT_IPC_TERM_BUFFER_LENGTH);
        }
        untetheredTimeouts(ctx);
        return ctx;
    }

    /**
     * Warns, on Linux, when {@code aeronDir} is not on tmpfs: the driver's log buffers, the tap's among them, are
     * memory-mapped files there, and writing back their dirty pages to a disk stalls the threads that write them.
     * @param component who is launching the driver
     * @param memberId  its node id
     * @param aeronDir  the driver's directory
     */
    public static void warnUnlessTmpfs(final Logger.Component component, final int memberId, final String aeronDir) {
        if (!SystemUtil.isLinux()) {
            return;
        }
        try {
            final String type = fileSystemType(Path.of(aeronDir));
            if (!"tmpfs".equals(type)) {
                Logger.log(component, Logger.Severity.Warn, Logger.CoreEventCode.Info, memberId,
                           "the Aeron directory %s is on %s, not tmpfs: page writeback of its log buffers can "
                               + "stall the tap — put it under /dev/shm",
                           aeronDir, type);
            }
        } catch (final IOException ex) {
            // The file system cannot be read; nothing to warn about.
        }
    }

    /**
     * The type of the file system holding {@code dir}, or its nearest existing ancestor's, since the driver
     * creates the directory.
     * @param dir a directory, existing or not
     */
    static String fileSystemType(final Path dir) throws IOException {
        Path existing = dir.toAbsolutePath();
        while (!Files.exists(existing)) {
            existing = existing.getParent();
        }
        return Files.getFileStore(existing).type();
    }

    /** Sets the untethered-subscriber timing where its Aeron property is not set. */
    static void untetheredTimeouts(final MediaDriver.Context ctx) {
        if (System.getProperty(Configuration.TIMER_INTERVAL_PROP_NAME) == null) {
            ctx.timerIntervalNs(DEFAULT_TIMER_INTERVAL_NS);
        }
        if (System.getProperty(Configuration.UNTETHERED_WINDOW_LIMIT_TIMEOUT_PROP_NAME) == null) {
            ctx.untetheredWindowLimitTimeoutNs(DEFAULT_UNTETHERED_TIMEOUT_NS);
        }
        if (System.getProperty(Configuration.UNTETHERED_LINGER_TIMEOUT_PROP_NAME) == null) {
            ctx.untetheredLingerTimeoutNs(DEFAULT_UNTETHERED_TIMEOUT_NS);
        }
    }
}
