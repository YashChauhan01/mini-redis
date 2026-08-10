package com.yash.miniredis.client;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hammers the server with concurrent SET/GET traffic and prints ops/sec.
 * Run it with increasing --threads to show throughput scaling as stripe
 * contention drops -- this is the evidence behind the "near-linear read
 * scalability" resume line, not just an assertion.
 *
 * Usage: java ... BenchmarkClient --host localhost --port 6380 --threads 8 --ops 20000
 */
public class BenchmarkClient {

    public static void main(String[] args) throws Exception {
        String host = strArg(args, "--host", "localhost");
        int port = intArg(args, "--port", 6380);
        int threads = intArg(args, "--threads", 8);
        int opsPerThread = intArg(args, "--ops", 20_000);
        int keySpace = intArg(args, "--keyspace", 10_000);
        double readRatio = 0.8; // 80% GET, 20% SET, a realistic-ish mix

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicLong completed = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(threads);
        long start = System.nanoTime();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try (Socket socket = new Socket(host, port);
                     BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                     BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {
                    in.readLine(); // consume the "+OK mini-redis ready" banner
                    for (int i = 0; i < opsPerThread; i++) {
                        int key = ThreadLocalRandom.current().nextInt(keySpace);
                        if (ThreadLocalRandom.current().nextDouble() < readRatio) {
                            out.write("GET k" + key);
                        } else {
                            out.write("SET k" + key + " v" + key);
                        }
                        out.write("\n");
                        out.flush();
                        in.readLine();
                        completed.incrementAndGet();
                    }
                } catch (IOException e) {
                    System.err.println("[bench] worker error: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
        pool.shutdown();

        long totalOps = completed.get();
        System.out.printf("[bench] threads=%d totalOps=%d time=%.2fs throughput=%.0f ops/sec%n",
                threads, totalOps, seconds, totalOps / seconds);
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
