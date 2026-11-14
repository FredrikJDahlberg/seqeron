package org.limitless.seqeron.proxy;

import io.aeron.Aeron;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;
import org.agrona.CloseHelper;

/**
 * Several hosts in one process ({@code proxy.hosts=a,b}), each a {@link ClientProxy} on a thread of its own, all on one
 * Aeron client (doc/seqeron-protocol-spec.md §17.6). A host that ends fenced or with the cluster unreachable, which
 * alone would exit 70, is rebuilt in place after a second: its client restarts and the others do not notice. Anything
 * else a host throws is the process's fault, for its owner to handle.
 */
public final class Hosts implements AutoCloseable {
    private static final long REBUILD_DELAY_MS = 1000;
    private static final long JOIN_MS = TimeUnit.SECONDS.toMillis(5);

    private final List<HostProperties> hosts;
    private final String aeronDir;
    private final List<Thread> threads = new ArrayList<>();
    /** Each host's proxy, written by its own thread when it rebuilds one and read by {@link #close}. */
    private final AtomicReferenceArray<ClientProxy> current;
    private Aeron aeron;
    private volatile boolean running = true;

    private Hosts(final List<HostProperties> hosts, final String aeronDir) {
        this.hosts = hosts;
        this.aeronDir = aeronDir;
        current = new AtomicReferenceArray<>(hosts.size());
    }

    /**
     * Reads and checks every host's properties, starting nothing.
     *
     * @throws IllegalArgumentException if {@code proxy.hosts} names none, or a host's properties are invalid or clash
     *     with another's
     */
    public static Hosts configure(final Properties properties) {
        final List<HostProperties> hosts = new ArrayList<>();
        final Set<String> hostNames = new HashSet<>();
        final Set<Integer> clientIds = new HashSet<>();
        final Set<String> snapshotDirs = new HashSet<>();
        String aeronDir = null;
        for (final String name : properties.getProperty("proxy.hosts", "").split(",")) {
            if (name.isBlank()) {
                continue;
            }
            final HostProperties host = new HostProperties(name.trim(), properties);
            final ClientProxy proxy = new ClientProxy(host);
            proxy.validate();
            require(hostNames.add(proxy.hostName()), "two hosts are named " + proxy.hostName());
            require(clientIds.add(proxy.clientId()), "two hosts take clientId " + proxy.clientId());
            require(snapshotDirs.add(proxy.snapshotDir()), "two hosts share snapshotDir " + proxy.snapshotDir());
            require(aeronDir == null || aeronDir.equals(proxy.aeronDir()),
                    "the hosts share one Aeron client, so one aeronDir: " + aeronDir + " and " + proxy.aeronDir());
            aeronDir = proxy.aeronDir();
            hosts.add(host);
        }
        require(!hosts.isEmpty(), "proxy.hosts names no host");
        return new Hosts(hosts, aeronDir);
    }

    /**
     * Connects the hosts' Aeron client and starts each host on its own thread.
     *
     * @param onFault run once if a host throws; the process should then stop
     */
    public Hosts start(final Runnable onFault) {
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        for (int i = 0; i < hosts.size(); i++) {
            final int index = i;
            final Thread thread = new Thread(() -> serve(index, onFault), "host-" + hosts.get(i).name());
            threads.add(thread);
            thread.start();
        }
        return this;
    }

    private void serve(final int index, final Runnable onFault) {
        while (running) {
            final ClientProxy proxy = new ClientProxy(hosts.get(index));
            current.set(index, proxy);
            if (!running) {
                return;
            }
            final int status;
            try {
                status = proxy.run(aeron);
            } catch (final Throwable ex) {
                System.err.printf("host %s failed: %s%n", hosts.get(index).name(), ex);
                ex.printStackTrace();
                onFault.run();
                return;
            }
            if (!running || status != ClientProxy.EXIT_RESTART) {
                return;
            }
            System.out.printf("host %s exited %d: rebuilding it%n", hosts.get(index).name(), status);
            for (final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(REBUILD_DELAY_MS);
                 running && System.nanoTime() < end;) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            }
        }
    }

    /** Stops every host, waits for its thread, and closes the hosts' Aeron client. */
    @Override
    public void close() {
        running = false;
        for (int i = 0; i < current.length(); i++) {
            final ClientProxy proxy = current.get(i);
            if (proxy != null) {
                proxy.stop();
            }
        }
        for (final Thread thread : threads) {
            try {
                thread.join(JOIN_MS);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        CloseHelper.quietClose(aeron);
    }

    private static void require(final boolean condition, final String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
