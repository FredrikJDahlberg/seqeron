package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class TradingDayTest {
    private static final long SECOND = 1_000_000_000L;

    @Test
    void parsesPhasesAndSkipsComments() {
        final TradingDay day =
            TradingDay.parse(List.of("# a comment", "", "open 2 100 300  # ramp", "flat 1 50 50"), 1);
        assertEquals(List.of(new TradingDay.Phase("open", 2, 100, 300), new TradingDay.Phase("flat", 1, 50, 50)),
                     day.phases());
        assertEquals(3 * SECOND, day.durationNs());
    }

    @Test
    void scaleMultipliesEveryRate() {
        final TradingDay day = TradingDay.parse(List.of("open 1 100 300"), 2.5);
        assertEquals(new TradingDay.Phase("open", 1, 250, 750), day.phases().get(0));
    }

    @Test
    void refusesWhatIsNotAPhase() {
        assertThrows(IllegalArgumentException.class, () -> TradingDay.parse(List.of("open 1 100"), 1));
        assertThrows(IllegalArgumentException.class, () -> TradingDay.parse(List.of("open 0 100 100"), 1));
        assertThrows(IllegalArgumentException.class, () -> TradingDay.parse(List.of("open 1 0 100"), 1));
        assertThrows(IllegalArgumentException.class, () -> TradingDay.parse(List.of("# nothing"), 1));
    }

    @Test
    void rateMovesLinearlyAcrossAPhase() {
        final TradingDay day = TradingDay.parse(List.of("open 2 100 300", "flat 1 50 50"), 1);
        assertEquals(0, day.phaseAt(0));
        assertEquals(100, day.rateAt(0), 1e-9);
        assertEquals(200, day.rateAt(SECOND), 1e-9);
        assertEquals(1, day.phaseAt(2 * SECOND));
        assertEquals(50, day.rateAt(2 * SECOND), 1e-9);
        assertEquals(1, day.phaseAt(10 * SECOND));
    }

    @Test
    void arrivalsMatchTheRateAndEndWithTheDay() {
        final TradingDay day = TradingDay.parse(List.of("flat 10 10000 10000", "ramp 10 5000 15000"), 1);
        final SplittableRandom random = new SplittableRandom(1);
        final long[] counts = new long[2];
        long offset = day.nextArrival(0, random);
        long last = 0;
        while (offset >= 0) {
            assertTrue(offset > last);
            counts[day.phaseAt(offset)]++;
            last = offset;
            offset = day.nextArrival(offset, random);
        }
        assertTrue(last < day.durationNs());
        assertEquals(100_000, counts[0], 2_000);
        assertEquals(100_000, counts[1], 2_000);
    }

    @Test
    void shippedProfilesParse() throws Exception {
        for (final String name : List.of("/trading-day-us.txt", "/trading-day-nordic.txt")) {
            final Path profile = Path.of(TradingDayTest.class.getResource(name).toURI());
            assertTrue(TradingDay.parse(Files.readAllLines(profile), 1).phases().size() > 5, name);
        }
    }

    @Test
    void histogramBucketsRoundTripWithinOneBucket() {
        for (long ns = 1; ns < 10 * SECOND; ns = ns * 3 + 1) {
            final long floor = TradingDayBench.bucketFloor(TradingDayBench.bucket(ns));
            assertTrue(floor <= ns && ns - floor <= ns / 32, ns + " -> " + floor);
        }
        final long[] hist = new long[60 * 32];
        hist[TradingDayBench.bucket(1_000)] = 99;
        hist[TradingDayBench.bucket(1_000_000)] = 1;
        assertEquals(992, TradingDayBench.percentile(hist, 0.5));
        assertTrue(TradingDayBench.percentile(hist, 1.0) > 960_000);
    }
}
