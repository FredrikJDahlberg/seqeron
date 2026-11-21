package org.limitless.seqeron.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

/**
 * A sink that writes on a thread of its own: {@link #record} queues the event and returns, and the logging thread
 * hands it to the wrapped sink. A full queue drops the event and counts it. A {@link Logger.Severity#Fault} event
 * waits, up to {@link #FAULT_WAIT_MS}, until it is written, since the process stops after one. A shutdown hook writes
 * what is still queued; after {@link #close} the wrapped sink is called on the reporting thread.
 */
public final class AsyncLoggerSink implements Logger.LoggerSink, AutoCloseable {
    /** The default sink's queue capacity, in events. */
    public static final int DEFAULT_CAPACITY = 4096;
    /** How long a {@link Logger.Severity#Fault} event waits to be written. */
    public static final long FAULT_WAIT_MS = 1000;

    private static final long IDLE_NS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long FAULT_POLL_NS = TimeUnit.MICROSECONDS.toNanos(100);

    private final Logger.LoggerSink sink;
    private final ManyToOneConcurrentArrayQueue<Logger.LoggerEvent> queue;
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread thread;
    private final Thread shutdownHook;

    /** Events taken off the queue and written, for a {@code Fault} event to wait on. */
    private volatile long written;
    private volatile boolean running = true;
    /** The last event dropped, whose component and member the "dropped" line reports under. */
    private volatile Logger.LoggerEvent lastDropped;

    /**
     * Starts the logging thread, a daemon, and registers the shutdown hook.
     * @param sink     where the logging thread writes each event
     * @param capacity how many events the queue holds
     */
    public AsyncLoggerSink(final Logger.LoggerSink sink, final int capacity) {
        this.sink = sink;
        this.queue = new ManyToOneConcurrentArrayQueue<>(capacity);
        this.thread = new Thread(this::run, "seqeron-logger");
        this.thread.setDaemon(true);
        this.thread.start();
        this.shutdownHook = new Thread(this::stop, "seqeron-logger-drain");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    @Override
    public void record(final Logger.LoggerEvent event) {
        if (!running) {
            sink.record(event);
            return;
        }
        if (!queue.offer(event)) {
            lastDropped = event;
            dropped.incrementAndGet();
            return;
        }
        if (event.severity() == Logger.Severity.Fault) {
            awaitWritten(queue.addedCount());
        }
    }

    /** Events dropped because the queue was full, since this sink was created. */
    public long dropped() {
        return dropped.get();
    }

    /** Writes what is queued, stops the logging thread and removes the shutdown hook. */
    @Override
    public void close() {
        stop();
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (final IllegalStateException ignore) {
            // the JVM is already shutting down, and the hook has run or is running
        }
    }

    private void stop() {
        if (closed.compareAndSet(false, true)) {
            running = false;
            LockSupport.unpark(thread);
            try {
                thread.join(FAULT_WAIT_MS);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void awaitWritten(final long count) {
        final long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(FAULT_WAIT_MS);
        LockSupport.unpark(thread);
        while (written < count && running && System.nanoTime() < deadlineNs) {
            LockSupport.parkNanos(FAULT_POLL_NS);
        }
    }

    private void run() {
        long reported = 0;
        while (running) {
            if (queue.drain(this::writeQueued) == 0) {
                reported = reportDrops(reported);
                LockSupport.parkNanos(IDLE_NS);
            }
        }
        queue.drain(this::writeQueued);
        reportDrops(reported);
    }

    private void writeQueued(final Logger.LoggerEvent event) {
        write(event);
        written++;
    }

    private void write(final Logger.LoggerEvent event) {
        try {
            sink.record(event);
        } catch (final RuntimeException ignore) {
            // a sink that throws loses its own event, not the logging thread
        }
    }

    /** Writes one line for the events dropped since the last one. */
    private long reportDrops(final long reported) {
        final long count = dropped.get();
        if (count != reported) {
            final Logger.LoggerEvent last = lastDropped;
            write(new Logger.LoggerEvent(last.component(), Logger.Severity.Warn, Logger.CoreEventCode.LogRecordsDropped,
                                         last.memberId(),
                                         (count - reported) + " log records dropped, the logging queue being full"));
        }
        return count;
    }
}
