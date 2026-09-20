package org.limitless.seqeron.util;

import java.util.Locale;
import java.util.function.Supplier;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;

/** The duty-cycle idle strategy a process names by system property. The C++ twin is {@code util/IdleStrategy.hpp}. */
public final class IdleStrategies {
    private IdleStrategies() {
    }

    /**
     * Resolves the strategy a system property names.
     *
     * @param property names {@code backoff} (the default), {@code yielding} or {@code busyspin}, case-insensitive
     * @return a factory for that strategy: one instance per agent thread, never shared
     */
    public static Supplier<IdleStrategy> fromProperty(final String property) {
        final String name = System.getProperty(property, "backoff");
        return switch (name.toLowerCase(Locale.ROOT)) {
            case "busyspin" -> BusySpinIdleStrategy::new;
            case "yielding" -> YieldingIdleStrategy::new;
            case "backoff" -> BackoffIdleStrategy::new;
            default -> throw new IllegalArgumentException("Unknown " + property + "=" + name +
                                                          " (expected 'backoff', 'yielding', or 'busyspin')");
        };
    }
}
