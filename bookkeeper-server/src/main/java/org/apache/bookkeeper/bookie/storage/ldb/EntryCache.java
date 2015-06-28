package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class EntryCache {
    private final ConcurrentNavigableMap<LongPair, ByteBuf> cache;
    private final AtomicLong size;
    private final AtomicLong count;

    public EntryCache() {
        this.cache = new ConcurrentSkipListMap<LongPair, ByteBuf>();
        this.size = new AtomicLong(0L);
        this.count = new AtomicLong(0L);
    }
    
    public void put(long ledgerId, long entryId, ByteBuf entry) {
        // Make a copy of the entry for the cache, to make sure we're not retaining a buffer much bigger than the actual
        // entry
        ByteBuf copiedEntry = PooledByteBufAllocator.DEFAULT.directBuffer(entry.readableBytes(), entry.readableBytes());
        entry.markReaderIndex();
        copiedEntry.writeBytes(entry);
        entry.resetReaderIndex();

        putNoCopy(ledgerId, entryId, copiedEntry);
        copiedEntry.release();
    }

    public void putNoCopy(long ledgerId, long entryId, ByteBuf entry) {
        entry.retain();
        ByteBuf oldValue = cache.put(new LongPair(ledgerId, entryId), entry);
        if (oldValue != null) {
            size.addAndGet(-oldValue.readableBytes());
            oldValue.release();
        }

        size.addAndGet(entry.readableBytes());
        count.incrementAndGet();
    }

    public ByteBuf get(long ledgerId, long entryId) {
        ByteBuf buffer = cache.get(new LongPair(ledgerId, entryId));
        if (buffer == null) {
            return null;
        } else {
            try {
                buffer.retain();
                return buffer;
            } catch (Throwable t) {
                // Buffer was already destroyed between get() and retain()
                return null;
            }
        }
    }

    public void invalidate(long ledgerId, long entryId) {
        ByteBuf buf = cache.remove(new LongPair(ledgerId, entryId));
        if (buf != null) {
            size.addAndGet(-buf.readableBytes());
            count.decrementAndGet();
            buf.release();
        }
    }

    public ByteBuf getLastEntry(long ledgerId) {
        // Next entry key is the first entry of the next ledger, we first seek to that entry and then step back to find
        // the last entry on ledgerId
        LongPair nextEntryKey = new LongPair(ledgerId + 1, 0);

        Entry<LongPair, ByteBuf> mapEntry = cache.headMap(nextEntryKey).lastEntry();
        if (mapEntry != null && mapEntry.getKey().first == ledgerId) {
            ByteBuf entry = mapEntry.getValue();
            try {
                entry.retain();
                return entry;
            } catch (Throwable t) {
                // Buffer was already destroyed between get() and retain()
                return null;
            }
        } else {
            return null;
        }
    }

    public long deleteLedger(long ledgerId) {
        return deleteEntries(new LongPair(ledgerId, 0), new LongPair(ledgerId + 1, 0), false);
    }

    public long trimLedger(long ledgerId, long lastEntryId) {
        return deleteEntries(new LongPair(ledgerId, 0), new LongPair(ledgerId, lastEntryId), true);
    }

    private long deleteEntries(LongPair start, LongPair end, boolean endIncluded) {
        NavigableMap<LongPair, ByteBuf> entriesToDelete = cache.subMap(start, true, end, endIncluded);

        long deletedSize = 0;
        long deletedCount = 0;
        while (true) {
            Entry<LongPair, ByteBuf> entry = entriesToDelete.pollFirstEntry();
            if (entry == null) {
                break;
            }

            deletedSize += entry.getValue().readableBytes();
            deletedCount++;
            entry.getValue().release();
        }

        size.addAndGet(-deletedSize);
        count.addAndGet(-deletedCount);
        return deletedSize;
    }

    public Set<Entry<LongPair, ByteBuf>> entries() {
        return cache.entrySet();
    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    public long size() {
        return size.get();
    }

    public long count() {
        return count.get();
    }

    public void clear() {
        while (true) {
            Entry<LongPair, ByteBuf> entry = cache.pollFirstEntry();
            if (entry == null) {
                break;
            }

            ByteBuf content = entry.getValue();
            size.addAndGet(-content.readableBytes());
            content.release();
        }
        count.set(0L);
    }
}
