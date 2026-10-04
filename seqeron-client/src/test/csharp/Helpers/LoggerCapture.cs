using System;
using Org.Limitless.Seqeron.Util;
using Xunit;

namespace Org.Limitless.Seqeron.Helpers;

/// <summary>A sink that hands each record to a callback.</summary>
internal sealed class CallbackLoggerSink : Logger.ILoggerSink
{
    private readonly Action<Logger.LoggerEvent> _record;

    public CallbackLoggerSink(Action<Logger.LoggerEvent> record)
    {
        _record = record;
    }

    public void Record(Logger.LoggerEvent loggerEvent)
    {
        _record(loggerEvent);
    }
}

/// <summary>The suites that install a sink: the logger is process-wide, so they run alone.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class LoggerCollection
{
    public const string Name = "Logger";
}
