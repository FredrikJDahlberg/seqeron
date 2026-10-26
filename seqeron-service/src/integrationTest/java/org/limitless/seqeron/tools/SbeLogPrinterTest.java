package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.Tests;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.FrameLayer;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.IrDecoder;

/**
 * The offline dump of a real member's archive: three members sequence the groups' registration, the heartbeats and
 * five probe frames (the even ones addressed to both groups), the cluster stops, and the printer reads what
 * member 0's archive kept.
 */
class SbeLogPrinterTest {
    private static final int COUNT = 5;
    private static final int MEMBER = 0;
    private static final int GROUP_1_STREAM = FrameLayer.queueStreamId(InProcessCluster.GROUP_ID);
    private static final int GROUP_2_STREAM = FrameLayer.queueStreamId(InProcessCluster.OTHER_GROUP_ID);

    @TempDir
    static Path baseDir;

    private static Path archiveDir;

    @BeforeAll
    static void sequenceFramesAndStop() {
        final InProcessCluster cluster = new InProcessCluster(baseDir);
        try {
            cluster.startAll();
            try (ConfirmProducer producer = new ConfirmProducer(cluster.aeronDir(MEMBER), MEMBER, COUNT, 0)) {
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (!producer.isDrained() && System.nanoTime() < deadline) {
                    producer.doWork();
                    Tests.yield();
                }
                assertTrue(producer.isDrained(), "the probe frames were not sequenced");
            }
        } finally {
            cluster.close();
        }
        archiveDir = baseDir.resolve("archive-" + MEMBER);
    }

    @AfterAll
    static void forgetArchive() {
        archiveDir = null;
    }

    private static List<Ir> bundledIrs() throws IOException {
        final List<Ir> irs = new ArrayList<>();
        for (final String name : List.of("frame", "replay", "probe", "sequencer")) {
            final String resource = "META-INF/seqeron/sbeir/sbe-" + name + ".sbeir";
            try (var in = SbeLogPrinterTest.class.getClassLoader().getResourceAsStream(resource)) {
                assertTrue(in != null, resource + " is on the classpath");
                try (IrDecoder decoder = new IrDecoder(ByteBuffer.wrap(in.readAllBytes()))) {
                    irs.add(decoder.decode());
                }
            }
        }
        return irs;
    }

    /** What the printer writes to stdout, and returns, for one dump. */
    private record Dump(boolean dumped, String stdout) {
        List<String> lines() {
            return stdout.lines().toList();
        }

        long count(final String text) {
            return lines().stream().filter(line -> line.contains(text)).count();
        }
    }

    private static Dump dump(final int streamIdFilter, final boolean oneLine) throws IOException {
        final PrintStream original = System.out;
        final ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try (PrintStream replacement = new PrintStream(captured, true)) {
            System.setOut(replacement);
            final boolean dumped = new SbeLogPrinter(bundledIrs(), archiveDir.toString(), streamIdFilter, oneLine,
                                                     SbeLogPrinter.NO_PAYLOAD_OUTPUT).scanAndDumpLog();
            replacement.flush();
            return new Dump(dumped, captured.toString());
        } finally {
            System.setOut(original);
        }
    }

    @Test
    @DisplayName("every recording is dumped, with the probe frames named and numbered")
    void dumpsEveryRecording() throws IOException {
        final Dump dump = dump(SbeLogPrinter.NO_STREAM_FILTER, true);
        assertTrue(dump.dumped());
        assertTrue(dump.count("[Catalog] Recording ID") >= 2, "one recording per queue stream:\n" + dump.stdout());
        assertEquals(COUNT + COUNT / 2, dump.count("ProbeMarker"),
                     "five frames on group 1's queue and the even ones on group 2's:\n" + dump.stdout());
        assertTrue(dump.count("GroupsRegistered") >= 2, dump.stdout());
        assertTrue(dump.count("LeadershipChanged") >= 2, dump.stdout());
    }

    @Test
    @DisplayName("naming a stream dumps its newest recording alone")
    void aStreamFilterSelectsOneQueue() throws IOException {
        final Dump first = dump(GROUP_1_STREAM, true);
        final Dump second = dump(GROUP_2_STREAM, true);
        assertEquals(1, first.count("[Catalog] Recording ID"));
        assertTrue(first.stdout().contains("Stream ID: " + GROUP_1_STREAM), first.stdout());
        assertEquals(COUNT, first.count("ProbeMarker"));
        assertTrue(second.stdout().contains("Stream ID: " + GROUP_2_STREAM), second.stdout());
        assertEquals(COUNT / 2, second.count("ProbeMarker"));
    }

    @Test
    @DisplayName("a stream no recording is on dumps nothing and says so")
    void anAbsentStreamIsNotDumped() throws IOException {
        final Dump dump = dump(4242, true);
        assertFalse(dump.dumped() && dump.count("[Catalog]") > 0, dump.stdout());
        assertEquals(0, dump.count("ProbeMarker"));
    }

    @Test
    @DisplayName("--oneline puts each message on a line of its own")
    void oneLineCollapsesEachMessage() throws IOException {
        final Dump multi = dump(GROUP_1_STREAM, false);
        final Dump single = dump(GROUP_1_STREAM, true);
        assertEquals(COUNT, single.count("ProbeMarker"));
        assertTrue(multi.lines().size() > single.lines().size(), "the same messages on more lines");
    }

    @Test
    @DisplayName("a directory with no catalog is reported, not dumped")
    void aMissingCatalogIsNotDumped() throws IOException {
        final Path empty = Files.createDirectory(baseDir.resolve("empty"));
        final PrintStream original = System.err;
        final ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try (PrintStream replacement = new PrintStream(captured, true)) {
            System.setErr(replacement);
            final boolean dumped = new SbeLogPrinter(bundledIrs(), empty.toString(), SbeLogPrinter.NO_STREAM_FILTER,
                                                     true, SbeLogPrinter.NO_PAYLOAD_OUTPUT).scanAndDumpLog();
            assertFalse(dumped);
            assertTrue(captured.toString().contains("Could not locate archive.catalog"), captured.toString());
        } finally {
            System.setErr(original);
        }
    }

    @Test
    @DisplayName("an archive directory that does not exist is refused")
    void aMissingDirectoryIsRefused() {
        final var error = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> new SbeLogPrinter(bundledIrs(), baseDir.resolve("nowhere").toString(),
                                    SbeLogPrinter.NO_STREAM_FILTER, true, SbeLogPrinter.NO_PAYLOAD_OUTPUT));
        assertTrue(error.getMessage().startsWith("Invalid archive directory"), error.getMessage());
    }
}
