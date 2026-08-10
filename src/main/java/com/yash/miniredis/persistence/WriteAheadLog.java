package com.yash.miniredis.persistence;

import com.yash.miniredis.store.KVStore;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Base64;

/**
 * Append-only log of every mutating command, written BEFORE it's applied to
 * the in-memory store. If the process dies mid-operation, the WAL is
 * replayed on the next startup and the store is reconstructed exactly.
 *
 * Keys/values are base64-encoded so arbitrary bytes (including our own
 * delimiter '|') never corrupt the line format.
 *
 * Durability trade-off, worth calling out in an interview: we flush() the
 * BufferedWriter after every write, which pushes bytes to the OS page cache
 * (survives a crash of our process) but does NOT fsync (won't survive a
 * power loss / kernel panic mid-write). A stricter build could
 * FileChannel.force(true) after every write for full fsync durability at
 * the cost of throughput -- classic Redis appendfsync always/everysec/no
 * trade-off.
 */
public class WriteAheadLog implements Closeable {
    private final Path path;
    private BufferedWriter writer;

    public WriteAheadLog(Path path) throws IOException {
        this.path = path;
        if (!Files.exists(path)) Files.createFile(path);
        openForAppend();
    }

    private void openForAppend() throws IOException {
        this.writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static String enc(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String dec(String s) {
        return new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
    }

    public synchronized void logSet(String key, String value, long expireAt) throws IOException {
        writer.write("SET|" + enc(key) + "|" + enc(value) + "|" + expireAt);
        writer.newLine();
        writer.flush();
    }

    public synchronized void logDelete(String key) throws IOException {
        writer.write("DEL|" + enc(key));
        writer.newLine();
        writer.flush();
    }

    public synchronized void logExpire(String key, long expireAt) throws IOException {
        writer.write("EXPIRE|" + enc(key) + "|" + expireAt);
        writer.newLine();
        writer.flush();
    }

    /** Replays every command in this WAL onto the given store. Used at startup. */
    public static int replay(Path path, KVStore store) throws IOException {
        if (!Files.exists(path)) return 0;
        int count = 0;
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] parts = line.split("\\|");
                try {
                    switch (parts[0]) {
                        case "SET" -> store.set(dec(parts[1]), dec(parts[2]), Long.parseLong(parts[3]));
                        case "DEL" -> store.delete(dec(parts[1]));
                        case "EXPIRE" -> store.expire(dec(parts[1]), Long.parseLong(parts[2]));
                        default -> System.err.println("[wal] skipping unknown entry: " + line);
                    }
                    count++;
                } catch (Exception ex) {
                    // A half-written last line (crash mid-append) is expected and safe to skip.
                    System.err.println("[wal] skipping corrupt/truncated line: " + line);
                }
            }
        }
        return count;
    }

    /** Wipes the WAL after a snapshot has captured all its state (compaction). */
    public synchronized void truncate() throws IOException {
        writer.close();
        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.WRITE)) {
            fc.truncate(0);
        }
        openForAppend();
    }

    @Override
    public synchronized void close() throws IOException {
        writer.close();
    }
}
