using System;
using System.Diagnostics;

namespace Org.Limitless.Seqeron.Util;

/// <summary>The clocks every duty-cycle deadline and delivery stamp in this tree reads.</summary>
public static class Clocks
{
    private static readonly long TicksPerMs = Stopwatch.Frequency / 1000;

    /// <summary>
    /// Elapsed milliseconds from an arbitrary origin, for measuring durations. <see cref="Stopwatch"/> is
    /// monotonic where the wall clock steps under NTP.
    /// </summary>
    /// <returns>a monotonic reading in milliseconds; meaningful only as a difference against another</returns>
    public static long MonotonicMs()
    {
        return Stopwatch.GetTimestamp() / TicksPerMs;
    }

    /// <summary>
    /// Wall-clock time in epoch nanoseconds, the unit the cluster's timestamps use. Agrona's
    /// <c>SystemEpochNanoClock</c> in Java; its resolution is the platform's, 100 ns at best.
    /// </summary>
    /// <returns>nanoseconds since 1970-01-01T00:00Z</returns>
    public static long EpochNanos()
    {
        return (DateTime.UtcNow.Ticks - DateTime.UnixEpoch.Ticks) * 100;
    }
}
