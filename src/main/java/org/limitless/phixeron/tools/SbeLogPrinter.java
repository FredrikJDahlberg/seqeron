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
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.IrDecoder;
import uk.co.real_logic.sbe.json.JsonPrinter;

/**
 * Offline dump of SBE messages recorded by Aeron Archive into an archive
 * directory (archive.catalog + segment files), printed as JSON using an
 * SBE IR (.sbeir) schema file.
 */
public class SbeLogPrinter {
    // firstRecordingDescriptorOffset for a non-legacy catalog; see io.aeron.archive.Catalog.
    private static final int CATALOG_HEADER_LENGTH = RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;
    private static final int DESCRIPTOR_HEADER_LENGTH = RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;

    private final File archiveDir;
    private final JsonPrinter jsonPrinter;
    private final StringBuilder outputBuilder = new StringBuilder();

    public SbeLogPrinter(final String specIrPath, final String archiveDirPath) throws Exception {
        this.archiveDir = new File(archiveDirPath);
        if (!archiveDir.exists() || !archiveDir.isDirectory()) {
            throw new IllegalArgumentException("Invalid archive directory: " + archiveDirPath);
        }

        try (IrDecoder irDecoder = new IrDecoder(specIrPath)) {
            final Ir ir = irDecoder.decode();
            this.jsonPrinter = new JsonPrinter(ir);
        }
    }

    public void scanAndDumpLog() {
        final File catalogFile = new File(archiveDir, "archive.catalog");
        if (!catalogFile.exists()) {
            System.err.println("Could not locate archive.catalog file in " + archiveDir);
            return;
        }

        try (RandomAccessFile raf = new RandomAccessFile(catalogFile, "r"); FileChannel channel = raf.getChannel()) {
            final long fileLength = channel.size();
            final ByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileLength);
            final UnsafeBuffer catalogBuffer = new UnsafeBuffer(byteBuffer);

            final RecordingDescriptorHeaderDecoder headerDecoder = new RecordingDescriptorHeaderDecoder();
            final RecordingDescriptorDecoder descriptorDecoder = new RecordingDescriptorDecoder();

            int offset = CATALOG_HEADER_LENGTH;
            boolean foundAny = false;

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
                    descriptorDecoder.wrap(catalogBuffer, offset + DESCRIPTOR_HEADER_LENGTH,
                                           RecordingDescriptorDecoder.BLOCK_LENGTH,
                                           RecordingDescriptorDecoder.SCHEMA_VERSION);

                    final long recordingId = descriptorDecoder.recordingId();
                    final long startPosition = descriptorDecoder.startPosition();
                    final long stopPosition = descriptorDecoder.stopPosition();
                    final int segmentFileLength = descriptorDecoder.segmentFileLength();

                    System.out.printf("[Catalog] Recording ID: %d | Start Pos: %d | Stop Pos: %d%n", recordingId,
                                      startPosition, stopPosition);

                    readPhysicalSegments(recordingId, startPosition, stopPosition, segmentFileLength);
                    foundAny = true;
                }

                offset += frameLength;
            }

            if (!foundAny) {
                System.err.println("No valid recordings found in catalog.");
            }

        } catch (Exception e) {
            System.err.println("Failed parsing catalog file: " + e.getMessage());
        }
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

                        outputBuilder.setLength(0);
                        jsonPrinter.print(outputBuilder, unsafeBuffer, sbePayloadOffset);

                        System.out.println("--- Log File Offset: " + currentPosition + " ---");
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

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: logprint.sh <sbe-ir-spec> <archive-dir>");
            System.exit(1);
        }

        try {
            final String specIrPath = args[0];
            final String archiveDirPath = args[1];

            final SbeLogPrinter printer = new SbeLogPrinter(specIrPath, archiveDirPath);
            printer.scanAndDumpLog();
        } catch (Exception e) {
            System.err.println("Execution failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
