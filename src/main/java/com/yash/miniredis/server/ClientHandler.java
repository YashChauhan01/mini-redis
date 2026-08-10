package com.yash.miniredis.server;

import com.yash.miniredis.persistence.WriteAheadLog;
import com.yash.miniredis.protocol.Protocol;
import com.yash.miniredis.store.KVStore;

import java.io.*;
import java.net.Socket;

/**
 * Handles exactly one client connection for its whole lifetime. This is the
 * "decouple I/O from command execution" piece: each handler just parses
 * text off the socket and calls into the shared, thread-safe KVStore --
 * it never does its own locking, so the concurrency story lives entirely
 * in KVStore/Stripe, not scattered across connection code.
 */
public class ClientHandler implements Runnable {
    private final Socket socket;
    private final KVStore store;
    private final WriteAheadLog wal;

    public ClientHandler(Socket socket, KVStore store, WriteAheadLog wal) {
        this.socket = socket;
        this.store = store;
        this.wal = wal;
    }

    @Override
    public void run() {
        try (socket;
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {

            out.write("+OK mini-redis ready\n");
            out.flush();

            String line;
            while ((line = in.readLine()) != null) {
                if (line.isBlank()) continue;
                String response = handle(line.trim());
                out.write(response);
                out.write("\n");
                out.flush();
                if ("QUIT".equalsIgnoreCase(line.trim())) break;
            }
        } catch (IOException e) {
            // Client disconnected abruptly -- not a server error, nothing to do.
        }
    }

    private String handle(String line) {
        String[] parts = line.split("\\s+");
        String cmd = parts[0].toUpperCase();
        try {
            return switch (cmd) {
                case "PING" -> Protocol.pong();
                case "SET" -> doSet(parts);
                case "GET" -> doGet(parts);
                case "DEL" -> doDel(parts);
                case "EXPIRE" -> doExpire(parts);
                case "STATS" -> doStats();
                case "QUIT" -> Protocol.ok();
                default -> Protocol.err("unknown command '" + cmd + "'");
            };
        } catch (ArrayIndexOutOfBoundsException e) {
            return Protocol.err("wrong number of arguments for '" + cmd + "'");
        } catch (NumberFormatException e) {
            return Protocol.err("value is not an integer or out of range");
        } catch (IOException e) {
            return Protocol.err("write-ahead log failure: " + e.getMessage());
        }
    }

    private String doSet(String[] parts) throws IOException {
        String key = parts[1];
        String value = parts[2];
        long expireAt = 0;
        if (parts.length >= 5 && "EX".equalsIgnoreCase(parts[3])) {
            long seconds = Long.parseLong(parts[4]);
            expireAt = System.currentTimeMillis() + seconds * 1000L;
        }
        // WAL-then-apply ordering matters: if we crash between these two
        // lines, replay reconstructs the SET on restart. If we applied to
        // the store first and crashed before logging, the write would be
        // silently lost on recovery.
        wal.logSet(key, value, expireAt);
        store.set(key, value, expireAt);
        return Protocol.ok();
    }

    private String doGet(String[] parts) {
        String value = store.get(parts[1]);
        return value == null ? Protocol.nil() : Protocol.bulk(value);
    }

    private String doDel(String[] parts) throws IOException {
        wal.logDelete(parts[1]);
        boolean removed = store.delete(parts[1]);
        return Protocol.integer(removed ? 1 : 0);
    }

    private String doExpire(String[] parts) throws IOException {
        long seconds = Long.parseLong(parts[2]);
        long expireAt = System.currentTimeMillis() + seconds * 1000L;
        wal.logExpire(parts[1], expireAt);
        boolean ok = store.expire(parts[1], expireAt);
        return Protocol.integer(ok ? 1 : 0);
    }

    private String doStats() {
        return Protocol.bulk("keys=" + store.size() + " stripes=" + store.stripeCount());
    }
}
