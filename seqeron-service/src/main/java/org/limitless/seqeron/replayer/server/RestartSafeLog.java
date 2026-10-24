package org.limitless.seqeron.replayer.server;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

/**
 * A member's own record of its cluster snapshots (doc/snapshot.md §10.8), in {@code restart-safe.log} in its
 * cluster directory. Local to the member and in no replicated state. A line is
 * <pre>
 *   cut &lt;logPosition&gt;    a snapshot was taken at that log position, which the member then hands to its Replayer
 *   safe &lt;logPosition&gt;   every co-located application that joined its round holds a confirmed file for it
 *   trimmed               {@code clusterctl trim-log} has run, so the Raft log no longer begins at its first term
 * </pre>
 * Each line is forced to disk before the call that wrote it returns. Written from the service thread ({@code cut})
 * and the Replayer's ({@code safe}), so every method is synchronized.
 */
public final class RestartSafeLog implements AutoCloseable {
    /** The file's name in the cluster directory. */
    public static final String FILE_NAME = "restart-safe.log";

    /** What a log holds. */
    public record State(Set<Long> cuts, Set<Long> safe, boolean trimmed) { }

    private final FileChannel channel;
    private final Set<Long> cuts;
    private final Set<Long> safe;
    private boolean trimmed;

    private RestartSafeLog(final FileChannel channel, final State state) {
        this.channel = channel;
        this.cuts = new HashSet<>(state.cuts());
        this.safe = new HashSet<>(state.safe());
        this.trimmed = state.trimmed();
    }

    /**
     * Opens the log in a cluster directory, creating it empty if there is none.
     * @param clusterDir the member's cluster directory
     * @return the log, positioned to append
     */
    public static RestartSafeLog open(final Path clusterDir) {
        try {
            Files.createDirectories(clusterDir);
            final Path file = clusterDir.resolve(FILE_NAME);
            final State state = read(file);
            return new RestartSafeLog(
                FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND),
                state);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Reads a log without opening it for writing, as the launcher does before the consensus module starts. A last
     * line a crash cut short is ignored.
     * @param file the log's file, which may not exist
     * @return what it holds
     */
    public static State read(final Path file) {
        final Set<Long> cuts = new HashSet<>();
        final Set<Long> safe = new HashSet<>();
        boolean trimmed = false;
        if (Files.exists(file)) {
            try {
                // Every line ends in a newline, so what follows the last one is a write a crash cut short.
                final String[] lines = Files.readString(file, StandardCharsets.US_ASCII).split("\n", -1);
                for (int i = 0; i < lines.length - 1; i++) {
                    final String[] words = lines[i].split(" ");
                    try {
                        if (words.length == 2 && words[0].equals("cut")) {
                            cuts.add(Long.parseLong(words[1]));
                        } else if (words.length == 2 && words[0].equals("safe")) {
                            safe.add(Long.parseLong(words[1]));
                        } else if (words.length == 1 && words[0].equals("trimmed")) {
                            trimmed = true;
                        }
                    } catch (final NumberFormatException torn) {
                        // A line a crash cut short.
                    }
                }
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
        return new State(cuts, safe, trimmed);
    }

    /** Records that a snapshot was taken at {@code logPosition}; durable on return. */
    public synchronized void appendCut(final long logPosition) {
        append("cut " + logPosition);
        cuts.add(logPosition);
    }

    /** Records that the snapshot at {@code logPosition} may be restarted from; durable on return. */
    public synchronized void appendSafe(final long logPosition) {
        append("safe " + logPosition);
        safe.add(logPosition);
    }

    /** Records that the Raft log has been trimmed; durable on return. */
    public synchronized void appendTrimmed() {
        append("trimmed");
        trimmed = true;
    }

    /** Whether a snapshot at {@code logPosition} was taken in this run or an earlier one. */
    public synchronized boolean isCut(final long logPosition) {
        return cuts.contains(logPosition);
    }

    /** Whether a snapshot at {@code logPosition} may be restarted from. */
    public synchronized boolean isSafe(final long logPosition) {
        return safe.contains(logPosition);
    }

    private void append(final String line) {
        try {
            final ByteBuffer bytes = ByteBuffer.wrap((line + "\n").getBytes(StandardCharsets.US_ASCII));
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
            channel.force(true);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public synchronized void close() {
        try {
            channel.close();
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
