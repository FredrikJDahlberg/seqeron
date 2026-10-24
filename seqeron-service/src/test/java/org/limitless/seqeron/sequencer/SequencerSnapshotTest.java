package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A {@link Sequencer} snapshotted at a random point of a seeded frame sequence, and a second restored from it, must
 * be the same state machine (doc/snapshot.md §10.2): every field it holds, and every frame it encodes from there on.
 * The sequence deposes sessions, leaves activations pending and requests rounds, so a field the snapshot misses
 * shows as a difference in a later frame or in the field itself.
 */
class SequencerSnapshotTest {
    private static final long TOPOLOGY_SESSION = SequencerTest.SESSION_ID + 1;
    private static final long PAYLOAD_SESSION = SequencerTest.SESSION_ID + 2;
    private static final int GATEWAYS = 4;

    /** Fields that are not state: scratch, codecs, and the snapshot's own bookkeeping. */
    private static final Set<String> NOT_STATE = Set.of("sink", "memberId", "encodeBuffer", "groupNamesBytes",
        "offeredGroupNames", "prologue", "snapshotSources", "snapshotSessions", "snapshotDesignated",
        "restoredSources", "restoredBindings", "restoredDesignations", "restoredRows", "restoredPending",
        "restoredTopology", "restored", "snapshotTopology");

    /** One step of a sequence, applied to a Sequencer. */
    private interface Step extends Consumer<Sequencer> { }

    private static Step ingress(final long sessionId, final long timestamp, final int length,
                                final java.util.function.Function<MutableDirectBuffer, Integer> encode) {
        return sequencer -> {
            final MutableDirectBuffer buffer = new ExpandableArrayBuffer(256);
            final int encoded = encode.apply(buffer);
            assertTrue(encoded > 0);
            sequencer.sequenceMessage(buffer, 0, encoded, sessionId, timestamp);
        };
    }

    /** A seeded sequence of {@code length} steps: topology first, then whatever the draws bring. */
    private static List<Step> sequence(final SplittableRandom random, final int length) {
        final List<Step> steps = new ArrayList<>();
        final long[] now = {SequencerTest.TIMESTAMP};
        steps.add(ingress(TOPOLOGY_SESSION, now[0], 0, b -> SequencerTest.encodeIngressGroupsRegistered(b, 0, "a", "b")));
        steps.add(sequencer -> sequencer.leadershipChanged(1, 0, now[0]));
        for (int row = 1; row <= GATEWAYS; row++) {
            final int id = row;
            final int sourceId = 9 + (row - 1) / 2; // two pairs: sources 9 and 10
            steps.add(ingress(TOPOLOGY_SESSION, now[0], 0,
                              b -> SequencerTest.encodeIngressGatewayRegistered(b, 0, id, sourceId, "GW-" + id,
                                                                                (id - 1) % 2, GATEWAYS - id)));
        }
        final int interval = 1 + random.nextInt(4);
        final int bytes = random.nextBoolean() ? 0 : 500;
        steps.add(ingress(TOPOLOGY_SESSION, now[0], 0,
                          b -> SequencerTest.encodeIngressSnapshotPolicy(b, 0, interval, bytes)));
        for (int i = 0; i < length; i++) {
            now[0] += random.nextInt(4) == 0 ? 7_000_000_000L : 300_000_000L * (1 + random.nextInt(6));
            final long timestamp = now[0];
            final int gatewayId = 1 + random.nextInt(GATEWAYS);
            final int sourceId = 9 + (gatewayId - 1) / 2;
            final long gatewaySession = 100 + gatewayId + 10L * random.nextInt(3);
            switch (random.nextInt(9)) {
                case 0 -> steps.add(ingress(gatewaySession, timestamp, 0,
                                            b -> SequencerTest.encodeIngressGatewayStarted(b, 0, gatewayId, sourceId)));
                case 1 -> steps.add(sequencer -> sequencer.sessionClosed(gatewaySession, timestamp));
                case 2 -> steps.add(ingress(TOPOLOGY_SESSION, timestamp, 0,
                                            b -> SequencerTest.encodeIngressActivationRequested(b, 0, gatewayId)));
                case 3 -> steps.add(ingress(TOPOLOGY_SESSION, timestamp, 0,
                                            b -> SequencerTest.encodeIngressSnapshotRequested(b, 0, 1)));
                case 4 -> {
                    final int term = 2 + random.nextInt(3);
                    final int leader = random.nextInt(3);
                    steps.add(sequencer -> sequencer.leadershipChanged(term, leader, timestamp));
                }
                case 5 -> steps.add(sequencer -> sequencer.sessionClosed(TOPOLOGY_SESSION, timestamp));
                case 6 -> steps.add(ingress(PAYLOAD_SESSION, timestamp, 0, b -> SequencerTest.encodeIngressPayload(b, 0)));
                default -> steps.add(sequencer -> sequencer.clusterHeartbeat(timestamp));
            }
        }
        return steps;
    }

    private static List<byte[]> snapshot(final Sequencer sequencer) {
        final List<byte[]> records = new ArrayList<>();
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer(2048);
        for (int index = 0;; index++) {
            final int length = sequencer.onSnapshot(buffer, index);
            if (length == 0) {
                return records;
            }
            final byte[] record = new byte[length];
            buffer.getBytes(0, record);
            records.add(record);
        }
    }

    private static Sequencer restored(final List<byte[]> records, final FrameRecorder recorder) {
        final Sequencer sequencer = new Sequencer(0, recorder);
        for (int index = 0; index < records.size(); index++) {
            final byte[] record = records.get(index);
            sequencer.onRestore(new org.agrona.concurrent.UnsafeBuffer(record), record.length, index,
                                sequencer.formatVersion());
        }
        return sequencer;
    }

    @Test
    @DisplayName("a Sequencer restored from a snapshot at any point of a seeded sequence is the same state machine")
    void aRestoredSequencerMatchesTheOriginal() {
        for (long seed = 1; seed <= 300; seed++) {
            final SplittableRandom random = new SplittableRandom(seed);
            final List<Step> steps = sequence(random, 60);
            final int cut = 6 + random.nextInt(steps.size() - 6);

            final FrameRecorder originalFrames = new FrameRecorder();
            final Sequencer original = new Sequencer(0, originalFrames);
            for (int i = 0; i < cut; i++) {
                steps.get(i).accept(original);
            }
            original.queueCut(1, 4096 * seed, 1 << 24, 1 << 12);
            final List<byte[]> records = snapshot(original);
            assertEquals(records.size(), snapshot(original).size(), "taking it changes nothing");

            final FrameRecorder restoredFrames = new FrameRecorder();
            final Sequencer restored = restored(records, restoredFrames);
            assertSameState("seed " + seed + " at " + cut, original, restored);
            assertArrayEquals(flatten(records), flatten(snapshot(restored)), "seed " + seed + ": it writes what it read");

            final int published = originalFrames.count();
            for (int i = cut; i < steps.size(); i++) {
                steps.get(i).accept(original);
                steps.get(i).accept(restored);
            }
            assertEquals(originalFrames.count() - published, restoredFrames.count(), "seed " + seed + ": frame count");
            for (int i = 0; i < restoredFrames.count(); i++) {
                assertArrayEquals(bytes(originalFrames.frame(published + i), originalFrames.length(published + i)),
                                  bytes(restoredFrames.frame(i), restoredFrames.length(i)),
                                  "seed " + seed + ": frame " + i + " after the cut");
                assertEquals(originalFrames.groupId(published + i), restoredFrames.groupId(i));
            }
            assertSameState("seed " + seed + " at the end", original, restored);
        }
    }

    @Test
    @DisplayName("a snapshot of nothing sequenced restores to nothing sequenced")
    void aSnapshotOfAnEmptySequencerRestoresToOne() {
        final Sequencer original = new Sequencer(0, new FrameRecorder());
        assertSameState("empty", original, restored(snapshot(original), new FrameRecorder()));
    }

    @Test
    @DisplayName("a snapshot is refused if it is a newer format, another schema, or its counts differ from the end's")
    void aSnapshotItCannotReadIsRefused() {
        final FrameRecorder frames = new FrameRecorder();
        final Sequencer original = new Sequencer(0, frames);
        for (final Step step : sequence(new SplittableRandom(7), 30)) {
            step.accept(original);
        }
        final List<byte[]> records = snapshot(original);

        final byte[] first = records.getFirst();
        assertThrows(IllegalStateException.class,
                     () -> new Sequencer(0, new FrameRecorder())
                               .onRestore(new org.agrona.concurrent.UnsafeBuffer(first), first.length, 0,
                                          original.formatVersion() + 1));

        final byte[] foreign = first.clone();
        foreign[4] = 0x7F; // schemaId, the third uint16 of the message header
        assertThrows(IllegalStateException.class,
                     () -> new Sequencer(0, new FrameRecorder())
                               .onRestore(new org.agrona.concurrent.UnsafeBuffer(foreign), foreign.length, 0,
                                          original.formatVersion()));

        final List<byte[]> short1 = new ArrayList<>(records);
        short1.remove(1); // an entry dropped: the end's counts no longer hold
        assertThrows(IllegalStateException.class, () -> restored(short1, new FrameRecorder()));
    }

    private static byte[] bytes(final DirectBuffer buffer, final int length) {
        final byte[] bytes = new byte[length];
        buffer.getBytes(0, bytes);
        return bytes;
    }

    private static byte[] flatten(final List<byte[]> records) {
        int total = 0;
        for (final byte[] record : records) {
            total += record.length;
        }
        final byte[] all = new byte[total];
        int at = 0;
        for (final byte[] record : records) {
            System.arraycopy(record, 0, all, at, record.length);
            at += record.length;
        }
        return all;
    }

    /** Every field that is state, compared by value, and the prologue's bytes. */
    private static void assertSameState(final String where, final Sequencer expected, final Sequencer actual) {
        try {
            for (final Field field : Sequencer.class.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) || NOT_STATE.contains(field.getName())
                    || field.getType().getName().contains("sbe.") && field.getType().getSimpleName().matches(".*(Encoder|Decoder)")) {
                    continue;
                }
                field.setAccessible(true);
                assertTrue(deepEquals(field.get(expected), field.get(actual)),
                           where + ": " + field.getName() + " differs: " + describe(field.get(expected)) + " vs "
                               + describe(field.get(actual)));
            }
            final Field prologue = Sequencer.class.getDeclaredField("prologue");
            final Field length = Sequencer.class.getDeclaredField("prologueLength");
            prologue.setAccessible(true);
            length.setAccessible(true);
            final int prologueLength = (int)length.get(expected);
            assertEquals(prologueLength, (int)length.get(actual), where + ": prologueLength");
            assertArrayEquals(bytes((DirectBuffer)prologue.get(expected), prologueLength),
                              bytes((DirectBuffer)prologue.get(actual), prologueLength), where + ": prologue");
        } catch (final ReflectiveOperationException ex) {
            throw new AssertionError(ex);
        }
    }

    private static boolean deepEquals(final Object a, final Object b) throws ReflectiveOperationException {
        if (a == null || b == null) {
            return a == b;
        }
        if (a.getClass().isArray()) {
            if (!b.getClass().isArray() || Array.getLength(a) != Array.getLength(b)) {
                return false;
            }
            for (int i = 0; i < Array.getLength(a); i++) {
                if (!deepEquals(Array.get(a, i), Array.get(b, i))) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof java.util.Map<?, ?> mapA) {
            final java.util.Map<?, ?> mapB = (java.util.Map<?, ?>)b;
            if (!mapA.keySet().equals(mapB.keySet())) {
                return false;
            }
            for (final java.util.Map.Entry<?, ?> entry : mapA.entrySet()) {
                if (!deepEquals(entry.getValue(), mapB.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof List<?> listA) {
            final List<?> listB = (List<?>)b;
            if (listA.size() != listB.size()) {
                return false;
            }
            for (int i = 0; i < listA.size(); i++) {
                if (!deepEquals(listA.get(i), listB.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (a.getClass().getName().startsWith("org.limitless.seqeron.sequencer.Sequencer$GatewayRow")) {
            for (final Field field : a.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                if (!deepEquals(field.get(a), field.get(b))) {
                    return false;
                }
            }
            return true;
        }
        return a.equals(b);
    }

    private static String describe(final Object value) {
        return value != null && value.getClass().isArray() ? Arrays.deepToString(new Object[] {value}) : String.valueOf(value);
    }
}
