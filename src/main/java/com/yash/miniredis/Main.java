/*
 * Problem Statement:
 *
 * Design and implement a lightweight, multithreaded,
 * in-memory key-value database inspired by Redis.
 *
 * The system should support multiple concurrent TCP clients
 * and provide the following capabilities:
 *
 * 1. Implement a TCP server capable of handling multiple
 *    simultaneous client connections.
 *
 * 2. Support basic key-value operations such as SET, GET,
 *    DELETE and EXPIRE through a custom text-based protocol.
 *
 * 3. Implement concurrent access using lock striping and
 *    read-write locks to reduce lock contention.
 *
 * 4. Support TTL-based key expiration through lazy expiration
 *    and a background expiration mechanism.
 *
 * 5. Implement approximate LRU-based memory eviction to
 *    maintain a bounded key capacity.
 *
 * 6. Implement persistence through an append-only
 *    Write-Ahead Log (WAL).
 *
 * 7. Support periodic snapshots and WAL replay to restore
 *    the database state after a restart.
 *
 * The implementation should prioritize concurrency,
 * memory management, and data recovery.
 */


package com.yash.miniredis;

import com.yash.miniredis.eviction.TTLSweeper;
import com.yash.miniredis.persistence.SnapshotManager;
import com.yash.miniredis.persistence.WriteAheadLog;
import com.yash.miniredis.server.TcpServer;
import com.yash.miniredis.store.KVStore;

import java.nio.file.Files;
import java.nio.file.Path;

public class Main {

    public static void main(String[] args) throws Exception {
        int port = intArg(args, "--port", 6380);
        int stripes = intArg(args, "--stripes", 32);
        int capacity = intArg(args, "--capacity", 100_000);
        int coreThreads = intArg(args, "--core-threads", 8);
        int maxThreads = intArg(args, "--max-threads", 64);
        long ttlSweepMs = intArg(args, "--ttl-sweep-ms", 1000);
        long snapshotIntervalMs = intArg(args, "--snapshot-interval-ms", 30_000);
        String dataDir = strArg(args, "--data-dir", "./data");

        Files.createDirectories(Path.of(dataDir));
        Path walPath = Path.of(dataDir, "mini-redis.wal");
        Path snapshotPath = Path.of(dataDir, "mini-redis.snapshot");

        KVStore store = new KVStore(stripes, capacity);

        System.out.println("[boot] recovering state from " + dataDir);

        WriteAheadLog wal = new WriteAheadLog(walPath);
        SnapshotManager snapshotManager = new SnapshotManager(store, wal, snapshotPath);

        int fromSnapshot = snapshotManager.loadSnapshot();
        int fromWal = WriteAheadLog.replay(walPath, store);
        System.out.println("[boot] recovered " + fromSnapshot + " key(s) from snapshot, replayed "
                + fromWal + " WAL entr" + (fromWal == 1 ? "y" : "ies") + " -- store now has "
                + store.size() + " key(s)");

        TTLSweeper sweeper = new TTLSweeper(store);
        sweeper.start(ttlSweepMs);

        snapshotManager.start(snapshotIntervalMs);

        TcpServer server = new TcpServer(port, store, wal, coreThreads, maxThreads, 256);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[shutdown] taking final snapshot before exit...");
            try {
                snapshotManager.takeSnapshot();
                sweeper.stop();
                snapshotManager.stop();
                server.stop();
                wal.close();
            } catch (Exception e) {
                System.err.println("[shutdown] error: " + e.getMessage());
            }
        }));

        server.start(); // blocks
    }

    private static int intArg(String[] args, String name, int def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) return Integer.parseInt(args[i + 1]);
        }
        return def;
    }

    private static String strArg(String[] args, String name, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) return args[i + 1];
        }
        return def;
    }
}
