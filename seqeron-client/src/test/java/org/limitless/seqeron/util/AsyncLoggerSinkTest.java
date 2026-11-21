package org.limitless.seqeron.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class AsyncLoggerSinkTest {
    private final List<Logger.LoggerEvent> written = new CopyOnWriteArrayList<>();
    private final List<Thread> writers = new CopyOnWriteArrayList<>();

    private void write(final Logger.LoggerEvent event) {
        writers.add(Thread.currentThread());
        written.add(event);
    }

    private static Logger.LoggerEvent event(final Logger.Severity severity, final String message) {
        return new Logger.LoggerEvent(Logger.CoreComponent.Sequencer, severity, Logger.CoreEventCode.Info, 1, message);
    }

    @Test
    void eventsAreWrittenInOrderOnTheLoggingThread() {
        try (AsyncLoggerSink sink = new AsyncLoggerSink(this::write, 16)) {
            for (int i = 0; i < 10; i++) {
                sink.record(event(Logger.Severity.Info, "line " + i));
            }
        }
        assertEquals(10, written.size());
        for (int i = 0; i < 10; i++) {
            assertEquals("line " + i, written.get(i).message());
            assertNotSame(Thread.currentThread(), writers.get(i));
        }
    }

    @Test
    void theCallerFormatsNothing() {
        final List<Thread> formatters = new CopyOnWriteArrayList<>();
        final Object argument = new Object() {
            @Override
            public String toString() {
                formatters.add(Thread.currentThread());
                return "argument";
            }
        };
        try (AsyncLoggerSink sink = new AsyncLoggerSink(this::write, 16)) {
            Logger.install(sink);
            Logger.info(Logger.CoreComponent.Sequencer, 1, "formatted %s", argument);
            assertTrue(formatters.isEmpty(), "formatted on the reporting thread");
        } finally {
            Logger.reset();
        }
        assertEquals("formatted argument", written.get(0).message());
    }

    @Test
    void aFullQueueDropsAndCountsAndReportsTheDrops() throws InterruptedException {
        final CountDownLatch blocked = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Logger.LoggerSink blocking = event -> {
            if (event.message().equals("block")) {
                blocked.countDown();
                await(release);
            }
            write(event);
        };
        try (AsyncLoggerSink sink = new AsyncLoggerSink(blocking, 4)) {
            sink.record(event(Logger.Severity.Info, "block"));
            assertTrue(blocked.await(5, TimeUnit.SECONDS));
            for (int i = 0; i < 10; i++) {
                sink.record(event(Logger.Severity.Error, "flood " + i)); // the reporting thread never waits
            }
            assertEquals(6, sink.dropped());
            release.countDown();
        }
        assertEquals(6, written.size(), "the blocking line, the four queued and the drop line");
        final Logger.LoggerEvent report = written.get(5);
        assertEquals(Logger.CoreEventCode.LogRecordsDropped, report.code());
        assertEquals(Logger.CoreComponent.Sequencer, report.component());
        assertTrue(report.message().startsWith("6 log records dropped"), report.message());
    }

    @Test
    void aFaultEventReturnsOnceWritten() {
        try (AsyncLoggerSink sink = new AsyncLoggerSink(this::write, 16)) {
            sink.record(event(Logger.Severity.Info, "before"));
            sink.record(event(Logger.Severity.Fault, "fatal"));
            assertEquals(2, written.size(), "a halt may follow a fault, so it is on the sink before record returns");
        }
    }

    @Test
    void aFaultEventWaitsNoLongerThanTheBound() throws InterruptedException {
        final CountDownLatch release = new CountDownLatch(1);
        try (AsyncLoggerSink sink = new AsyncLoggerSink(event -> await(release), 16)) {
            final long startNs = System.nanoTime();
            sink.record(event(Logger.Severity.Fault, "fatal"));
            final long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
            assertTrue(waitedMs >= AsyncLoggerSink.FAULT_WAIT_MS - 1 && waitedMs < 3 * AsyncLoggerSink.FAULT_WAIT_MS,
                       waitedMs + "ms");
            release.countDown();
        }
    }

    @Test
    void afterCloseTheSinkIsCalledOnTheReportingThread() {
        final AsyncLoggerSink sink = new AsyncLoggerSink(this::write, 16);
        sink.close();
        sink.record(event(Logger.Severity.Info, "late"));
        assertEquals(1, written.size());
        assertSame(Thread.currentThread(), writers.get(0));
    }

    @Test
    void aSinkThatThrowsLosesOnlyItsOwnEvent() {
        final Logger.LoggerSink throwing = event -> {
            if (event.message().equals("throw")) {
                throw new IllegalStateException("sink failed");
            }
            write(event);
        };
        try (AsyncLoggerSink sink = new AsyncLoggerSink(throwing, 16)) {
            sink.record(event(Logger.Severity.Info, "throw"));
            sink.record(event(Logger.Severity.Fault, "after"));
        }
        assertEquals(1, written.size());
        assertEquals("after", written.get(0).message());
    }

    private static void await(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
