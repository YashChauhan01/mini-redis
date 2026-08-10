package com.yash.miniredis.store;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * One shard of the key space. The whole point of striping is that N stripes
 * each guard 1/N of the keys with their own lock, so threads touching
 * different stripes never block each other -- only threads hashing to the
 * SAME stripe contend.
 *
 * Design note on LRU + RW-locks:
 * A textbook LRU (doubly-linked list, move node to front on every access)
 * needs a *structural* mutation on every GET, which would force GET to take
 * the write lock and destroy read concurrency -- defeating the purpose of a
 * read/write lock in the first place.
 *
 * Real Redis solves this the same way: it doesn't maintain exact LRU order.
 * Instead, every read stamps the entry with lastAccessTime (a cheap volatile
 * write, no lock needed for correctness of that field). When a stripe is
 * over capacity, eviction samples a handful of random entries and evicts
 * whichever one is *staler*, rather than scanning for the global least-
 * recently-used key. This is "approximated LRU" -- close enough in practice,
 * and it keeps GET lock-cheap.
 */
public class Stripe {
    private final ConcurrentHashMap<String, Entry> data = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final int capacity;
    private static final int EVICTION_SAMPLE_SIZE = 5;

    public Stripe(int capacity) {
        this.capacity = capacity;
    }

    public String get(String key) {
        lock.readLock().lock();
        Entry e;
        try {
            e = data.get(key);
            if (e == null) return null;
            if (e.isExpired(System.currentTimeMillis())) {
                e = null; // fall through to eviction below, outside read lock
            } else {
                e.touch();
                return e.value;
            }
        } finally {
            lock.readLock().unlock();
        }
        // Lazily remove the expired key. Can't upgrade read->write lock with
        // ReentrantReadWriteLock, so we re-acquire under the write lock and
        // re-check (another thread may have already removed/replaced it).
        lock.writeLock().lock();
        try {
            Entry cur = data.get(key);
            if (cur != null && cur.isExpired(System.currentTimeMillis())) {
                data.remove(key);
            }
        } finally {
            lock.writeLock().unlock();
        }
        return null;
    }

    public void set(String key, String value, long expireAt) {
        lock.writeLock().lock();
        try {
            if (!data.containsKey(key) && data.size() >= capacity) {
                evictOne();
            }
            data.put(key, new Entry(value, expireAt));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean delete(String key) {
        lock.writeLock().lock();
        try {
            return data.remove(key) != null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean expire(String key, long expireAt) {
        lock.writeLock().lock();
        try {
            Entry e = data.get(key);
            if (e == null) return false;
            e.expireAt = expireAt;
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Must be called while already holding the write lock. */
    private void evictOne() {
        if (data.isEmpty()) return;
        List<String> keys = new ArrayList<>(data.keySet());
        String victim = null;
        long oldest = Long.MAX_VALUE;
        int n = Math.min(EVICTION_SAMPLE_SIZE, keys.size());
        for (int i = 0; i < n; i++) {
            String k = keys.get(ThreadLocalRandom.current().nextInt(keys.size()));
            Entry e = data.get(k);
            if (e != null && e.lastAccessTime < oldest) {
                oldest = e.lastAccessTime;
                victim = k;
            }
        }
        if (victim != null) data.remove(victim);
    }

    /** Called periodically by the background sweeper. Bounded, non-blocking-ish scan. */
    public int sweepExpired() {
        long now = System.currentTimeMillis();
        int removed = 0;
        lock.writeLock().lock();
        try {
            for (Map.Entry<String, Entry> me : data.entrySet()) {
                if (me.getValue().isExpired(now)) {
                    data.remove(me.getKey());
                    removed++;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        return removed;
    }

    public int size() {
        return data.size();
    }

    /** Snapshot of all live (non-expired) entries, for persistence. */
    public List<Map.Entry<String, Entry>> snapshotEntries() {
        lock.readLock().lock();
        try {
            long now = System.currentTimeMillis();
            List<Map.Entry<String, Entry>> out = new ArrayList<>();
            for (Map.Entry<String, Entry> me : data.entrySet()) {
                if (!me.getValue().isExpired(now)) out.add(me);
            }
            return out;
        } finally {
            lock.readLock().unlock();
        }
    }
}
