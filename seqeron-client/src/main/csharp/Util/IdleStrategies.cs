using System;
using Adaptive.Agrona.Concurrent;

namespace Org.Limitless.Seqeron.Util;

/// <summary>
/// The duty-cycle idle strategy a process names in <see cref="EnvIdleStrategy"/>, as a C++ client does
/// (<c>util/IdleStrategy.hpp</c>); a Java process names it by system property instead.
/// </summary>
public static class IdleStrategies
{
    /// <summary>Names <c>backoff</c> (the default), <c>yielding</c> or <c>busyspin</c>, case-insensitive.</summary>
    public const string EnvIdleStrategy = "SEQERON_IDLE_STRATEGY";

    // Agrona.NET's defaults park for up to 16 ms; Java's for at most 1 ms, which this keeps.
    private const long MaxParkPeriodMs = 1;

    /// <summary>Resolves the strategy <see cref="EnvIdleStrategy"/> names.</summary>
    /// <returns>a factory for that strategy: one instance per agent thread, never shared</returns>
    /// <exception cref="ArgumentException">if the variable names no known strategy</exception>
    public static Func<IIdleStrategy> FromEnvironment()
    {
        string name = Environment.GetEnvironmentVariable(EnvIdleStrategy);
        name = string.IsNullOrEmpty(name) ? "backoff" : name;
        switch (name.ToLowerInvariant())
        {
            case "busyspin":
                return () => new BusySpinIdleStrategy();
            case "yielding":
                return () => new YieldingIdleStrategy();
            case "backoff":
                return () => new BackoffIdleStrategy(Configuration.IDLE_MAX_SPINS, Configuration.IDLE_MAX_YIELDS,
                                                     Configuration.IDLE_MIN_PARK_MS, MaxParkPeriodMs);
            default:
                throw new ArgumentException($"Unknown {EnvIdleStrategy}={name} " +
                                            "(expected 'backoff', 'yielding', or 'busyspin')");
        }
    }
}
