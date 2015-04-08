package org.apache.bookkeeper.bookie;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Records the total size, remaining size and the set of ledgers that comprise a entry log.
 */
public class EntryLogMetadata {
    private final long entryLogId;
    private long totalSize;
    private long remainingSize;
    private ConcurrentHashMap<Long, Long> ledgersMap;

    public EntryLogMetadata(long logId) {
        this.entryLogId = logId;

        totalSize = remainingSize = 0;
        ledgersMap = new ConcurrentHashMap<Long, Long>();
    }

    public void addLedgerSize(long ledgerId, long size) {
        totalSize += size;
        remainingSize += size;
        Long ledgerSize = ledgersMap.get(ledgerId);
        if (null == ledgerSize) {
            ledgerSize = 0L;
        }
        ledgerSize += size;
        ledgersMap.put(ledgerId, ledgerSize);
    }

    public void removeLedger(long ledgerId) {
        Long size = ledgersMap.remove(ledgerId);
        if (null == size) {
            return;
        }
        remainingSize -= size;
    }

    public boolean containsLedger(long ledgerId) {
        return ledgersMap.containsKey(ledgerId);
    }

    public double getUsage() {
        if (totalSize == 0L) {
            return 0.0f;
        }
        return (double) remainingSize / totalSize;
    }

    public boolean isEmpty() {
        return ledgersMap.isEmpty();
    }

    public long getEntryLogId() {
        return entryLogId;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getRemainingSize() {
        return remainingSize;
    }

    Map<Long, Long> getLedgersMap() {
        return ledgersMap;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ totalSize = ").append(totalSize).append(", remainingSize = ").append(remainingSize)
                .append(", ledgersMap = ").append(ledgersMap).append(" }");
        return sb.toString();
    }

}
