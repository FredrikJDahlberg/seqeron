package org.limitless.phixeron.tools;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.archive.codecs.RecordingState;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.phixeron.util.Logger;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.IrDecoder;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.json.JsonPrinter;
import uk.co.real_logic.sbe.otf.OtfHeaderDecoder;

/**
 * Offline dump of SBE messages recorded by Aeron Archive into an archive
 * directory (archive.catalog + segment files), printed as JSON using an
 * SBE IR (.sbeir) schema file.
 *
 * <p>By default every valid recording in the catalog is dumped. An archive holds more than the
 * sequenced tap — the cluster log is recorded alongside it and decodes against a different schema —
 * and a node restart mints a <em>new</em> tap recording rather than extending the old one, so the
 * default output interleaves unrelated schemas and repeated history. Pass a stream id to select
 * instead; see {@link #scanAndDumpLog()}.
 */
public class SbeLogPrinter {
    // firstRecordingDescriptorOffset for a non-legacy catalog; see io.aeron.archive.Catalog.
    private static final int CATALOG_HEADER_LENGTH = RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;
    private static final int DESCRIPTOR_HEADER_LENGTH = RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;

    /** streamIdFilter value meaning "no filter" — dump every recording. Stream id 0 is legal, so 0 cannot serve. */
    public static final int NO_STREAM_FILTER = Integer.MIN_VALUE;

    private final File archiveDir;
    private final Ir ir;
    private final JsonPrinter jsonPrinter;
    // JsonPrinter emits field values only, so the message name is read separately off each frame header.
    private final OtfHeaderDecoder sbeHeaderDecoder;
    private final int streamIdFilter;
    private final boolean oneLine;
    private final StringBuilder outputBuilder = new StringBuilder();

    public SbeLogPrinter(final String specIrPath, final String archiveDirPath, final int streamIdFilter,
                         final boolean oneLine) {
        this.archiveDir = new File(archiveDirPath);
        if (!archiveDir.exists() || !archiveDir.isDirectory()) {
            throw new IllegalArgumentException("Invalid archive directory: " + archiveDirPath);
        }
        this.streamIdFilter = streamIdFilter;
        this.oneLine = oneLine;

        try (IrDecoder irDecoder = new IrDecoder(specIrPath)) {
            this.ir = irDecoder.decode();
            this.jsonPrinter = new JsonPrinter(ir);
            this.sbeHeaderDecoder = new OtfHeaderDecoder(ir.headerStructure());
        }
    }

    /**
     * Message name for a template id, for labelling the dump — a header-only message such as {@code Tick}
     * is otherwise indistinguishable from any other in the JSON, which carries field values only. The first
     * token of a message is its BEGIN_MESSAGE token, whose name is the message name.
     */
    private String messageName(final int templateId) {
        final List<Token> tokens = ir.getMessage(templateId);
        return null == tokens || tokens.isEmpty() ? "<unknown>" : tokens.getFirst().name();
    }

    /**
     * Collapses the pretty-printed JSON onto a single line, replacing each newline and the indent run that
     * follows it with one space. JsonPrinter has no compact mode — the layout is hardcoded in JsonTokenListener
     * — so this rewrites its output. Safe because every literal newline in that output is structural: a newline
     * inside a string value is escaped to {@code \n} by {@code Types.jsonEscape}, never emitted raw.
     */
    private static void collapse(final StringBuilder builder) {
        int write = 0;
        for (int read = 0, length = builder.length(); read < length; read++) {
            final char c = builder.charAt(read);
            if ('\n' == c) {
                while (read + 1 < length && ' ' == builder.charAt(read + 1)) {
                    read++;
                }
                // A key already ends in ": ", so a second space there would only pad the output.
                if (write > 0 && ' ' != builder.charAt(write - 1)) {
                    builder.setCharAt(write++, ' ');
                }
            } else {
                builder.setCharAt(write++, c);
            }
        }
        // A trailing newline would leave a dangling space; nothing else can produce one at the end.
        if (write > 0 && ' ' == builder.charAt(write - 1)) {
            write--;
        }
        builder.setLength(write);
    }

    /**
     * Dumps recordings as JSON. With no stream filter every valid recording is dumped in catalog order.
     * With a stream filter only the <em>newest</em> recording on that stream is dumped: recording ids are
     * assigned monotonically by the archive, so the highest id on a stream is the most recent, and for the
     * sequenced tap that one recording holds the complete history — a node replays its whole cluster log on
     * restart and re-emits every message, so each tap recording starts again at globalSeqNo 1 and older ones
     * are strict prefixes of it. Dumping them all would just repeat that history.
     *
     * @return true if at least one recording was dumped.
     */
    public boolean scanAndDumpLog() {
        final File catalogFile = new File(archiveDir, "archive.catalog");
        if (!catalogFile.exists()) {
            System.err.println("Could not locate archive.catalog file in " + archiveDir);
            return false;
        }

        try (RandomAccessFile raf = new RandomAccessFile(catalogFile, "r"); FileChannel channel = raf.getChannel()) {
            final long fileLength = channel.size();
            final ByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileLength);
            final UnsafeBuffer catalogBuffer = new UnsafeBuffer(byteBuffer);

            final RecordingDescriptorHeaderDecoder headerDecoder = new RecordingDescriptorHeaderDecoder();
            final RecordingDescriptorDecoder descriptorDecoder = new RecordingDescriptorDecoder();

            int offset = CATALOG_HEADER_LENGTH;
            boolean foundAny = false;
            // Descriptor offset of the newest recording matching streamIdFilter, resolved after the walk so
            // "newest" can be decided across the whole catalog. Unused when no filter is set.
            int selectedOffset = -1;
            long selectedRecordingId = -1;

            while (offset + DESCRIPTOR_HEADER_LENGTH <= fileLength) {
                headerDecoder.wrap(catalogBuffer, offset, RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
                                   RecordingDescriptorHeaderDecoder.SCHEMA_VERSION);

                final int recordingLength = headerDecoder.length();
                if (recordingLength <= 0) {
                    break;
                }

                final int frameLength
                    = BitUtil.align(recordingLength + DESCRIPTOR_HEADER_LENGTH, BitUtil.CACHE_LINE_LENGTH);

                if (headerDecoder.state() == RecordingState.VALID) {
                    final int descriptorOffset = offset + DESCRIPTOR_HEADER_LENGTH;
                    descriptorDecoder.wrap(catalogBuffer, descriptorOffset, RecordingDescriptorDecoder.BLOCK_LENGTH,
                                           RecordingDescriptorDecoder.SCHEMA_VERSION);

                    if (NO_STREAM_FILTER == streamIdFilter) {
                        dumpRecording(descriptorDecoder);
                        foundAny = true;
                    } else if (descriptorDecoder.streamId() == streamIdFilter
                               && descriptorDecoder.recordingId() > selectedRecordingId) {
                        selectedRecordingId = descriptorDecoder.recordingId();
                        selectedOffset = descriptorOffset;
                    }
                }

                offset += frameLength;
            }

            if (NO_STREAM_FILTER != streamIdFilter && selectedOffset >= 0) {
                descriptorDecoder.wrap(catalogBuffer, selectedOffset, RecordingDescriptorDecoder.BLOCK_LENGTH,
                                       RecordingDescriptorDecoder.SCHEMA_VERSION);
                dumpRecording(descriptorDecoder);
                foundAny = true;
            }

            if (!foundAny) {
                System.err.println(NO_STREAM_FILTER == streamIdFilter
                                       ? "No valid recordings found in catalog."
                                       : "No valid recording on stream " + streamIdFilter + " found in catalog.");
            }

            return foundAny;

        } catch (Exception e) {
            System.err.println("Failed parsing catalog file: " + e.getMessage());
            return false;
        }
    }

    private void dumpRecording(final RecordingDescriptorDecoder descriptorDecoder) {
        final long recordingId = descriptorDecoder.recordingId();
        final long startPosition = descriptorDecoder.startPosition();
        final long stopPosition = descriptorDecoder.stopPosition();
        final int segmentFileLength = descriptorDecoder.segmentFileLength();

        System.out.printf("[Catalog] Recording ID: %d | Stream ID: %d | Start Pos: %d | Stop Pos: %d%n", recordingId,
                          descriptorDecoder.streamId(), startPosition, stopPosition);

        readPhysicalSegments(recordingId, startPosition, stopPosition, segmentFileLength);
    }

    private void readPhysicalSegments(final long recordingId, final long startPos, final long stopPos,
                                      final int segmentLength) {
        long currentPosition = startPos;
        // An in-progress recording has no stop position yet; rely on frame/segment EOF to end the scan.
        final long effectiveStopPos = stopPos == AeronArchive.NULL_POSITION ? Long.MAX_VALUE : stopPos;

        while (currentPosition < effectiveStopPos) {
            final long segmentBasePosition = currentPosition - (currentPosition % segmentLength);
            // Archive.Configuration.RECORDING_SEGMENT_SUFFIX is package-private, so it's inlined here.
            final File segmentFile = new File(archiveDir, recordingId + "-" + segmentBasePosition + ".rec");

            if (!segmentFile.exists()) {
                break;
            }

            final long positionBeforeSegment = currentPosition;

            try (RandomAccessFile raf = new RandomAccessFile(segmentFile, "r");
                 FileChannel channel = raf.getChannel()) {
                final long fileLength = channel.size();
                final ByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileLength);
                final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
                final DataHeaderFlyweight frameHeader = new DataHeaderFlyweight();

                int fileOffset = (int)(currentPosition % segmentLength);

                while (fileOffset < fileLength && currentPosition < effectiveStopPos) {
                    frameHeader.wrap(unsafeBuffer, fileOffset, (int)fileLength - fileOffset);

                    final int frameLength = frameHeader.frameLength();
                    if (frameLength <= 0) {
                        break;
                    }

                    if (frameHeader.headerType() == DataHeaderFlyweight.HDR_TYPE_DATA) {
                        final int sbePayloadOffset = fileOffset + DataHeaderFlyweight.HEADER_LENGTH;
                        final int templateId = sbeHeaderDecoder.getTemplateId(unsafeBuffer, sbePayloadOffset);

                        outputBuilder.setLength(0);
                        jsonPrinter.print(outputBuilder, unsafeBuffer, sbePayloadOffset);
                        if (oneLine) {
                            collapse(outputBuilder);
                        }
                        System.out.println("--- Log File Offset: " + currentPosition + " | "
                                           + messageName(templateId) + " (templateId " + templateId + ") ---");
                        System.out.println(outputBuilder);
                    }

                    final int paddedLength = BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
                    fileOffset += paddedLength;
                    currentPosition += paddedLength;
                }

            } catch (Exception e) {
                System.err.println("Exception parsing segment " + segmentFile + ": " + e.getMessage());
                break;
            }

            // No frame was available to advance past — the tail of a still-growing recording. Stop here.
            if (currentPosition == positionBeforeSegment) {
                break;
            }
        }
    }

    private static void usage() {
        System.out.println("Usage: sbe-log-printer.sh <sbe-ir-spec> <archive-dir> [--stream <id>] [--oneline]");
        System.out.println("  --stream <id>  dump only the newest recording on that stream");
        System.out.println("  --oneline      print each message as a single line of JSON");
    }

    public static void main(String[] args) {
        String specIrPath = null;
        String archiveDirPath = null;
        int streamIdFilter = NO_STREAM_FILTER;
        boolean oneLine = false;

        for (int i = 0; i < args.length; i++) {
            if ("--oneline".equals(args[i])) {
                oneLine = true;
            } else if ("--stream".equals(args[i])) {
                if (++i == args.length) {
                    usage();
                    System.exit(1);
                }
                try {
                    streamIdFilter = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    System.err.println("Not a stream id: " + args[i]);
                    System.exit(1);
                }
            } else if (specIrPath == null) {
                specIrPath = args[i];
            } else if (archiveDirPath == null) {
                archiveDirPath = args[i];
            } else {
                System.err.println("Unexpected argument: " + args[i]);
                usage();
                System.exit(1);
            }
        }

        if (specIrPath == null || archiveDirPath == null) {
            usage();
            System.exit(1);
        }

        try {
            final SbeLogPrinter printer = new SbeLogPrinter(specIrPath, archiveDirPath, streamIdFilter, oneLine);
            if (!printer.scanAndDumpLog()) {
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Execution failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
