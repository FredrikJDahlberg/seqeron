package org.limitless.seqeron.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * A trading day's message rate as a run of phases, for {@link TradingDayBench}. Each phase lasts its seconds and moves
 * its rate linearly from one value to another, so a flat phase, a ramp and a decay are each one line, and a curve is a
 * few. Arrivals are Poisson at the rate in force when the previous one arrived.
 *
 * <p>A profile is text, one phase a line, {@code #} starting a comment: {@code <name> <seconds> <from> <to>}, the
 * rates in messages a second.
 */
final class TradingDay {
    /** One phase: its rate goes from {@code fromRate} to {@code toRate} over {@code seconds}. */
    record Phase(String name, double seconds, double fromRate, double toRate) { }

    private final List<Phase> phases;
    private final long[] endNs;

    private TradingDay(final List<Phase> phases) {
        this.phases = List.copyOf(phases);
        endNs = new long[phases.size()];
        double seconds = 0;
        for (int i = 0; i < phases.size(); i++) {
            seconds += phases.get(i).seconds();
            endNs[i] = (long)(seconds * TimeUnit.SECONDS.toNanos(1));
        }
    }

    /**
     * @param lines the profile
     * @param scale multiplies every rate
     * @throws IllegalArgumentException on a line that is not a phase, or a profile with none
     */
    static TradingDay parse(final List<String> lines, final double scale) {
        final List<Phase> phases = new ArrayList<>();
        for (final String raw : lines) {
            final int comment = raw.indexOf('#');
            final String line = (comment < 0 ? raw : raw.substring(0, comment)).strip();
            if (line.isEmpty()) {
                continue;
            }
            final String[] fields = line.split("\\s+");
            if (fields.length != 4) {
                throw new IllegalArgumentException("not <name> <seconds> <from> <to>: " + raw);
            }
            final double seconds = Double.parseDouble(fields[1]);
            final double from = Double.parseDouble(fields[2]) * scale;
            final double to = Double.parseDouble(fields[3]) * scale;
            if (!(seconds > 0) || !(from > 0) || !(to > 0)) {
                throw new IllegalArgumentException("seconds and rates must be positive: " + raw);
            }
            phases.add(new Phase(fields[0], seconds, from, to));
        }
        if (phases.isEmpty()) {
            throw new IllegalArgumentException("no phases");
        }
        return new TradingDay(phases);
    }

    List<Phase> phases() {
        return phases;
    }

    long durationNs() {
        return endNs[endNs.length - 1];
    }

    /** The phase in force at {@code offsetNs} from the day's start; the last one at or past its end. */
    int phaseAt(final long offsetNs) {
        for (int i = 0; i < endNs.length; i++) {
            if (offsetNs < endNs[i]) {
                return i;
            }
        }
        return endNs.length - 1;
    }

    /** The rate in force at {@code offsetNs}, in messages a second. */
    double rateAt(final long offsetNs) {
        final int i = phaseAt(offsetNs);
        final Phase phase = phases.get(i);
        final long startNs = i == 0 ? 0 : endNs[i - 1];
        final double progress = Math.min(1.0, (double)(offsetNs - startNs) / (endNs[i] - startNs));
        return phase.fromRate() + (phase.toRate() - phase.fromRate()) * progress;
    }

    /** The next arrival after one at {@code offsetNs}, or -1 when it falls past the day's end. */
    long nextArrival(final long offsetNs, final SplittableRandom random) {
        final double gapSeconds = -Math.log(1.0 - random.nextDouble()) / rateAt(offsetNs);
        final long next = offsetNs + Math.max(1, (long)(gapSeconds * TimeUnit.SECONDS.toNanos(1)));
        return next < durationNs() ? next : -1;
    }
}
