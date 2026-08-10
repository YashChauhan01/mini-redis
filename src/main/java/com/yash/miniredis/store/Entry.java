package com.yash.miniredis.store;

/**
 * A single stored value plus its metadata.
 *
 * expireAt: epoch millis at which this entry becomes invalid. 0 means "no TTL".
 * lastAccessTime: updated (as a plain volatile write, no locking) on every GET.
 *   Used for *approximate* LRU eviction -- see Stripe for why we don't maintain
 *   a strict doubly-linked-list LRU order under the read lock.
 */
public class Entry {
    public final String value;
    public volatile long expireAt;       // 0 = never expires
    public volatile long lastAccessTime; // epoch millis, updated on read

    public Entry(String value, long expireAt) {
        this.value = value;
        this.expireAt = expireAt;
        this.lastAccessTime = System.currentTimeMillis();
    }

    public boolean isExpired(long now) {
        return expireAt != 0 && now >= expireAt;
    }

    public void touch() {
        this.lastAccessTime = System.currentTimeMillis();
    }
}
