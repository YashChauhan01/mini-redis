package com.yash.miniredis.eviction;

import com.yash.miniredis.store.KVStore;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Catches TTL'd keys that nobody ever reads again (so lazy on-read expiry
 * alone would never evict them, leaking memory forever). Runs on its own
 * daemon thread, briefly write-locking each stripe in turn -- never the
 * whole store at once, so foreground GET/SET on other stripes are
 * unaffected while a sweep is in progress.
 */
public class TTLSweeper {
    private final KVStore store;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ttl-sweeper");
                t.setDaemon(true);
                return t;
            });

    public TTLSweeper(KVStore store) {
        this.store = store;
    }

    public void start(long intervalMs) {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                int removed = store.sweepExpired();
                if (removed > 0) {
                    System.out.println("[ttl-sweeper] evicted " + removed + " expired key(s)");
                }
            } catch (Exception e) {
                System.err.println("[ttl-sweeper] error: " + e.getMessage());
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }
}
