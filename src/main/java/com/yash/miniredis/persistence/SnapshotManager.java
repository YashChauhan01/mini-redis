package com.yash.miniredis.persistence;

import com.yash.miniredis.store.Entry;
import com.yash.miniredis.store.KVStore;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically dumps the FULL current state to disk, then truncates the WAL
 * (compaction). This bounds recovery time: on restart we only ever replay
 * "WAL entries since the last snapshot" instead of the store's entire
 * history since day one, which would otherwise grow unbounded.
 *
 * Recovery on startup = load snapshot (if any) -> replay WAL on top of it.
 */
public class SnapshotManager {
    private final KVStore store;
    private final WriteAheadLog wal;
    private final Path snapshotPath;
    private final Path snapshotTmpPath;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "snapshot-manager");
                t.setDaemon(true);
                return t;
            });

    public SnapshotManager(KVStore store, WriteAheadLog wal, Path snapshotPath) {
        this.store = store;
        this.wal = wal;
        this.snapshotPath = snapshotPath;
        this.snapshotTmpPath = Path.of(snapshotPath.toString() + ".tmp");
    }

    public void start(long intervalMs) {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                takeSnapshot();
            } catch (Exception e) {
                System.err.println("[snapshot] failed: " + e.getMessage());
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    /** Write full state to a tmp file, atomically rename over the real snapshot, then compact the WAL. */
    public synchronized void takeSnapshot() throws IOException {
        List<Map.Entry<String, Entry>> entries = store.allEntries();
        try (BufferedWriter w = Files.newBufferedWriter(snapshotTmpPath, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (Map.Entry<String, Entry> e : entries) {
                w.write(enc(e.getKey()) + "|" + enc(e.getValue().value) + "|" + e.getValue().expireAt);
                w.newLine();
            }
        }
        // Atomic rename so a crash mid-write never leaves a half-written snapshot
        // in place of a good one.
        Files.move(snapshotTmpPath, snapshotPath, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);

        // Everything currently in the snapshot no longer needs replaying from
        // the WAL, so we can safely wipe it -- this is what bounds recovery time.
        wal.truncate();
        System.out.println("[snapshot] wrote " + entries.size() + " key(s), compacted WAL");
    }

    /** Loads the snapshot file (if it exists) into the store. Call before WAL replay on startup. */
    public int loadSnapshot() throws IOException {
        if (!Files.exists(snapshotPath)) return 0;
        int count = 0;
        try (BufferedReader r = Files.newBufferedReader(snapshotPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = r.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] parts = line.split("\\|");
                store.set(dec(parts[0]), dec(parts[1]), Long.parseLong(parts[2]));
                count++;
            }
        }
        return count;
    }

    private static String enc(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String dec(String s) {
        return new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
    }
}
