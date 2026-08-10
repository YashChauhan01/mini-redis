package com.yash.miniredis.server;

import com.yash.miniredis.persistence.WriteAheadLog;
import com.yash.miniredis.store.KVStore;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;

/**
 * Accepts TCP connections and hands each one to a bounded thread pool.
 *
 * "Bounded" is the important word: an unbounded pool (or one thread per
 * connection with no cap) lets a burst of clients exhaust server memory /
 * OS threads. Here, once maxThreads are busy and the backlog queue is full,
 * CallerRunsPolicy makes the ACCEPTING thread itself run the handler --
 * which naturally throttles the accept loop instead of dropping
 * connections or OOM-ing, a simple but effective backpressure mechanism.
 */
public class TcpServer {
    private final int port;
    private final KVStore store;
    private final WriteAheadLog wal;
    private final ThreadPoolExecutor pool;
    private volatile boolean running = true;
    private ServerSocket serverSocket;

    public TcpServer(int port, KVStore store, WriteAheadLog wal, int coreThreads, int maxThreads, int queueCapacity) {
        this.port = port;
        this.store = store;
        this.wal = wal;
        this.pool = new ThreadPoolExecutor(
                coreThreads, maxThreads,
                60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                r -> {
                    Thread t = new Thread(r, "client-handler");
                    t.setDaemon(false);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        System.out.println("[server] listening on port " + port +
                " (pool: core=" + pool.getCorePoolSize() + " max=" + pool.getMaximumPoolSize() + ")");
        while (running) {
            try {
                Socket client = serverSocket.accept();
                pool.execute(new ClientHandler(client, store, wal));
            } catch (IOException e) {
                if (running) System.err.println("[server] accept failed: " + e.getMessage());
            }
        }
    }

    public void stop() throws IOException {
        running = false;
        pool.shutdown();
        if (serverSocket != null) serverSocket.close();
    }
}
