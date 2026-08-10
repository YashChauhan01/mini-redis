package com.yash.miniredis.store;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

/**
 * Top-level, thread-safe key-value store. Routes each key to one of
 * numStripes independent Stripe shards by hash, so unrelated keys never
 * contend on the same lock. This is what eliminates the classic
 * "single global lock" bottleneck of a naive synchronized HashMap.
 */
public class KVStore {
    private final Stripe[] stripes;
    private final int numStripes;

    public KVStore(int numStripes, int totalCapacity) {
        this.numStripes = numStripes;
        this.stripes = new Stripe[numStripes];
        int perStripeCapacity = Math.max(1, totalCapacity / numStripes);
        for (int i = 0; i < numStripes; i++) {
            stripes[i] = new Stripe(perStripeCapacity);
        }
    }

    private Stripe stripeFor(String key) {
        // spread() guards against poor hashCode() distributions clustering
        // keys onto the same stripe, same trick ConcurrentHashMap itself uses.
        int h = spread(key.hashCode());
        return stripes[h % numStripes];
    }

    private static int spread(int h) {
        h ^= (h >>> 16);
        return h & 0x7fffffff;
    }

    public String get(String key) {
        return stripeFor(key).get(key);
    }

    public void set(String key, String value, long expireAt) {
        stripeFor(key).set(key, value, expireAt);
    }

    public boolean delete(String key) {
        return stripeFor(key).delete(key);
    }

    public boolean expire(String key, long expireAt) {
        return stripeFor(key).expire(key, expireAt);
    }

    public int sweepExpired() {
        int total = 0;
        for (Stripe s : stripes) total += s.sweepExpired();
        return total;
    }

    public int size() {
        int total = 0;
        for (Stripe s : stripes) total += s.size();
        return total;
    }

    public int stripeCount() {
        return numStripes;
    }

    /** Full point-in-time snapshot of all live entries, for persistence. */
    public List<Map.Entry<String, Entry>> allEntries() {
        List<Map.Entry<String, Entry>> out = new ArrayList<>();
        for (Stripe s : stripes) out.addAll(s.snapshotEntries());
        return out;
    }
}
