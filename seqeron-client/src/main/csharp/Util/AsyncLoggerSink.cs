using System;
using System.Threading;
using System.Threading.Channels;

namespace Org.Limitless.Seqeron.Util;

/// <summary>
/// A sink that writes on a thread of its own: <see cref="Record"/> queues the event and returns, and the logging
/// thread hands it to the wrapped sink. A full queue drops the event and counts it. A
/// <see cref="Logger.Severity.Fault"/> event waits, up to <see cref="FaultWaitMs"/>, until it is written, since the
/// process stops after one. Process exit writes what is still queued; after <see cref="Dispose"/> the wrapped sink
/// is called on the reporting thread.
/// </summary>
public sealed class AsyncLoggerSink : Logger.ILoggerSink, IDisposable
{
    /// <summary>The default sink's queue capacity, in events.</summary>
    public const int DefaultCapacity = 4096;

    /// <summary>How long a <see cref="Logger.Severity.Fault"/> event waits to be written.</summary>
    public const int FaultWaitMs = 1000;

    private const int IdleMs = 1;

    // A Fault event carries the signal its reporter waits on.
    private readonly record struct Entry(Logger.LoggerEvent Event, ManualResetEventSlim Written);

    private readonly Logger.ILoggerSink _sink;
    private readonly Channel<Entry> _queue;
    private readonly Thread _thread;
    private long _dropped;
    private int _closed;
    private volatile bool _running = true;
    // The last event dropped, whose component and member the "dropped" line reports under.
    private volatile Logger.LoggerEvent _lastDropped;

    /// <summary>Starts the logging thread, a background one, and drains the queue at process exit.</summary>
    /// <param name="sink">where the logging thread writes each event</param>
    /// <param name="capacity">how many events the queue holds</param>
    public AsyncLoggerSink(Logger.ILoggerSink sink, int capacity)
    {
        _sink = sink;
        _queue = Channel.CreateBounded<Entry>(
            new BoundedChannelOptions(capacity) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true });
        _thread = new Thread(Run) { Name = "seqeron-logger", IsBackground = true };
        _thread.Start();
        AppDomain.CurrentDomain.ProcessExit += OnProcessExit;
    }

    /// <inheritdoc/>
    public void Record(Logger.LoggerEvent loggerEvent)
    {
        if (!_running)
        {
            _sink.Record(loggerEvent);
            return;
        }
        ManualResetEventSlim written =
            loggerEvent.Severity == Logger.Severity.Fault ? new ManualResetEventSlim(false) : null;
        if (!_queue.Writer.TryWrite(new Entry(loggerEvent, written)))
        {
            _lastDropped = loggerEvent;
            Interlocked.Increment(ref _dropped);
            return;
        }
        if (written != null)
        {
            written.Wait(FaultWaitMs);
        }
    }

    /// <summary>Events dropped because the queue was full, since this sink was created.</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>Writes what is queued, stops the logging thread and stops draining at process exit.</summary>
    public void Dispose()
    {
        Stop();
        AppDomain.CurrentDomain.ProcessExit -= OnProcessExit;
    }

    private void OnProcessExit(object sender, EventArgs args)
    {
        Stop();
    }

    private void Stop()
    {
        if (Interlocked.Exchange(ref _closed, 1) == 0)
        {
            _running = false;
            _thread.Join(FaultWaitMs);
        }
    }

    private void Run()
    {
        long reported = 0;
        while (_running)
        {
            if (!Drain())
            {
                reported = ReportDrops(reported);
                Thread.Sleep(IdleMs);
            }
        }
        Drain();
        ReportDrops(reported);
    }

    /// <returns>whether it wrote anything</returns>
    private bool Drain()
    {
        bool any = false;
        while (_queue.Reader.TryRead(out Entry entry))
        {
            any = true;
            Write(entry.Event);
            entry.Written?.Set();
        }
        return any;
    }

    private void Write(Logger.LoggerEvent loggerEvent)
    {
        try
        {
            _sink.Record(loggerEvent);
        }
        catch (Exception)
        {
            // a sink that throws loses its own event, not the logging thread
        }
    }

    // Writes one line for the events dropped since the last one.
    private long ReportDrops(long reported)
    {
        long count = Dropped;
        if (count != reported)
        {
            Logger.LoggerEvent last = _lastDropped;
            Write(new Logger.LoggerEvent(last.Component, Logger.Severity.Warn, Logger.CoreEventCode.LogRecordsDropped,
                                         last.MemberId,
                                         (count - reported) + " log records dropped, the logging queue being full"));
        }
        return count;
    }
}
