using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Org.Limitless.Seqeron.Helpers;
using Xunit;

namespace Org.Limitless.Seqeron.Util;

[Collection(LoggerCollection.Name)]
public class AsyncLoggerSinkTest
{
    private readonly ConcurrentQueue<(Logger.LoggerEvent Event, Thread Writer)> _written = new();

    private void Write(Logger.LoggerEvent loggerEvent)
    {
        _written.Enqueue((loggerEvent, Thread.CurrentThread));
    }

    private static Logger.LoggerEvent Event(Logger.Severity severity, string message)
    {
        return new Logger.LoggerEvent(Logger.CoreComponent.Sequencer, severity, Logger.CoreEventCode.Info, 1, message);
    }

    [Fact]
    public void EventsAreWrittenInOrderOnTheLoggingThread()
    {
        using (var sink = new AsyncLoggerSink(new CallbackLoggerSink(Write), 16))
        {
            for (int i = 0; i < 10; i++)
            {
                sink.Record(Event(Logger.Severity.Info, "line " + i));
            }
        }
        var written = _written.ToArray();
        Assert.Equal(10, written.Length);
        for (int i = 0; i < 10; i++)
        {
            Assert.Equal("line " + i, written[i].Event.Message);
            Assert.NotSame(Thread.CurrentThread, written[i].Writer);
        }
    }

    private sealed class FormattingProbe
    {
        public readonly ConcurrentQueue<Thread> Formatters = new();

        public override string ToString()
        {
            Formatters.Enqueue(Thread.CurrentThread);
            return "argument";
        }
    }

    [Fact]
    public void TheCallerFormatsNothing()
    {
        var argument = new FormattingProbe();
        try
        {
            using var sink = new AsyncLoggerSink(new CallbackLoggerSink(Write), 16);
            Logger.Install(sink);
            Logger.Info(Logger.CoreComponent.Sequencer, 1, "formatted {0}", argument);
            Assert.Empty(argument.Formatters); // nothing formatted on the reporting thread
        }
        finally
        {
            Logger.Reset();
        }
        var written = _written.ToArray();
        Assert.Equal("formatted argument", written[0].Event.Message);
    }

    [Fact]
    public void AFullQueueDropsAndCountsAndReportsTheDrops()
    {
        using var blocked = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        var blocking = new CallbackLoggerSink(e =>
                                              {
                                                  if (e.Message == "block")
                                                  {
                                                      blocked.Set();
                                                      release.Wait();
                                                  }
                                                  Write(e);
                                              });
        using (var sink = new AsyncLoggerSink(blocking, 4))
        {
            sink.Record(Event(Logger.Severity.Info, "block"));
            Assert.True(blocked.Wait(5000, TestContext.Current.CancellationToken));
            for (int i = 0; i < 10; i++)
            {
                sink.Record(Event(Logger.Severity.Error, "flood " + i)); // the reporting thread never waits
            }
            Assert.Equal(6, sink.Dropped);
            release.Set();
        }
        var written = _written.ToArray();
        Assert.Equal(6, written.Length); // the blocking line, the four queued and the drop line
        Logger.LoggerEvent report = written[5].Event;
        Assert.Equal(Logger.CoreEventCode.LogRecordsDropped, report.Code);
        Assert.Equal(Logger.CoreComponent.Sequencer, report.Component);
        Assert.StartsWith("6 log records dropped", report.Message);
    }

    [Fact]
    public void AFaultEventReturnsOnceWritten()
    {
        using var sink = new AsyncLoggerSink(new CallbackLoggerSink(Write), 16);
        sink.Record(Event(Logger.Severity.Info, "before"));
        sink.Record(Event(Logger.Severity.Fault, "fatal"));
        Assert.Equal(2, _written.Count); // a halt may follow a fault, so it is on the sink before Record returns
    }

    [Fact]
    public void AFaultEventWaitsNoLongerThanTheBound()
    {
        using var release = new ManualResetEventSlim(false);
        var waiting = new CallbackLoggerSink(
            _ => release.Wait());
        using (var sink = new AsyncLoggerSink(waiting, 16))
        {
            var watch = Stopwatch.StartNew();
            sink.Record(Event(Logger.Severity.Fault, "fatal"));
            long waitedMs = watch.ElapsedMilliseconds;
            Assert.InRange(waitedMs, AsyncLoggerSink.FaultWaitMs - 20, 3 * AsyncLoggerSink.FaultWaitMs);
            release.Set();
        }
    }

    [Fact]
    public void AfterDisposeTheSinkIsCalledOnTheReportingThread()
    {
        var sink = new AsyncLoggerSink(new CallbackLoggerSink(Write), 16);
        sink.Dispose();
        sink.Record(Event(Logger.Severity.Info, "late"));
        var written = _written.ToArray();
        Assert.Single(written);
        Assert.Same(Thread.CurrentThread, written[0].Writer);
    }

    [Fact]
    public void ASinkThatThrowsLosesOnlyItsOwnEvent()
    {
        var throwing = new CallbackLoggerSink(e =>
                                              {
                                                  if (e.Message == "throw")
                                                  {
                                                      throw new InvalidOperationException("sink failed");
                                                  }
                                                  Write(e);
                                              });
        using (var sink = new AsyncLoggerSink(throwing, 16))
        {
            sink.Record(Event(Logger.Severity.Info, "throw"));
            sink.Record(Event(Logger.Severity.Fault, "after"));
        }
        var written = _written.ToArray();
        Assert.Single(written);
        Assert.Equal("after", written.Single().Event.Message);
    }
}
