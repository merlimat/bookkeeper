/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.bookie;

import static com.google.common.base.Charsets.UTF_8;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.PerChannelBookieClient;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap.BiConsumerLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;

/**
 * This class manages the writing of the bookkeeper entries. All the new
 * entries are written to a common log. The LedgerCache will have pointers
 * into files created by this class with offsets into the files to find
 * the actual ledger entry. The entry log files created by this class are
 * identified by a long.
 */
public class EntryLogger {
    private static final Logger LOG = LoggerFactory.getLogger(EntryLogger.class);

    private static class BufferedLogChannel extends BufferedChannel {
        final private long logId;
        private final EntryLogMetadata entryLogMetada;

        public BufferedLogChannel(FileChannel fc, int writeCapacity,
                                  int readCapacity, long logId) throws IOException {
            super(fc, writeCapacity, readCapacity);
            this.logId = logId;
            this.entryLogMetada = new EntryLogMetadata(logId);
        }
        public long getLogId() {
            return logId;
        }

        public void registerWrittenEntry(long ledgerId, long entrySize) {
            entryLogMetada.addLedgerSize(ledgerId, entrySize);
        }

        public ConcurrentLongLongHashMap getLedgersMap() {
            return entryLogMetada.getLedgersMap();
        }
    }

    volatile File currentDir;
    private final LedgerDirsManager ledgerDirsManager;
    private final AtomicBoolean shouldCreateNewEntryLog = new AtomicBoolean(false);

    private volatile long leastUnflushedLogId;

    /**
     * The maximum size of a entry logger file.
     */
    final long logSizeLimit;
    private List<BufferedLogChannel> logChannelsToFlush;
    private volatile BufferedLogChannel logChannel;
    private final EntryLoggerAllocator entryLoggerAllocator;
    private final boolean entryLogPreAllocationEnabled;
    private final CopyOnWriteArrayList<EntryLogListener> listeners
        = new CopyOnWriteArrayList<EntryLogListener>();

    private enum HeaderVersion {
        V0(0), // Old log file format (no index)
        V1(1), // New format, contains ledger map index

        Unknown(-1); // Unrecognized header version

        final int value;

        private HeaderVersion(int value) {
            this.value = value;
        }
    }

    private static class Header {
        final HeaderVersion version;
        final long ledgersMapOffset;
        final int ledgersCount;

        Header(HeaderVersion version, long ledgersMapOffset, int ledgersCount) {
            this.version = version;
            this.ledgersMapOffset = ledgersMapOffset;
            this.ledgersCount = ledgersCount;
        }
    }

    /**
     * The 1K block at the head of the entry logger file
     * that contains the fingerprint and meta-data.
     *
     * Header is composed of:
     * Fingerprint: 4 bytes "BKLO"
     * Log file HeaderVersion enum: 4 bytes
     * Ledger map offset: 8 bytes
     * Ledgers Count: 4 bytes
     */
    final static int LOGFILE_HEADER_SIZE = 1024;
    final ByteBuf LOGFILE_HEADER = Unpooled.buffer(LOGFILE_HEADER_SIZE);

    final static int HEADER_VERSION_POSITION = 4;
    final static int LEDGERS_MAP_OFFSET_POSITION = HEADER_VERSION_POSITION + 4;

    /**
     * Ledgers map is composed of multiple parts that can be split into separated entries. Each of them is composed of:
     *
     * <pre>
     * length: (4 bytes) [0-3]
     * ledger id (-1): (8 bytes) [4 - 11]
     * entry id: (8 bytes) [12-19]
     * num ledgers stored in current metadata entry: (4 bytes) [20 - 23]
     * ledger entries: sequence of (ledgerid, size) (8 + 8 bytes each) [24..]
     * </pre>
     */
    final static int LEDGERS_MAP_HEADER_SIZE = 4 + 8 + 8 + 4;
    final static int LEDGERS_MAP_ENTRY_SIZE = 8 + 8;

    // Break the ledgers map into multiple batches, each of which can contain up to 10K ledgers
    final static int LEDGERS_MAP_MAX_BATCH_SIZE = 10000;

    final static long INVALID_LID = -1L;

    // EntryId used to mark an entry (belonging to INVALID_ID) as a component of the serialized ledgers map
    final static long LEDGERS_MAP_ENTRY_ID = -2L;

    final static int MIN_SANE_ENTRY_SIZE = 8 + 8;
    final static int MAX_SANE_ENTRY_SIZE = PerChannelBookieClient.MAX_FRAME_LENGTH;
    final static long MB = 1024 * 1024;

    final ServerConfiguration conf;
    /**
     * Scan entries in a entry log file.
     */
    public static interface EntryLogScanner {
        /**
         * Tests whether or not the entries belongs to the specified ledger
         * should be processed.
         *
         * @param ledgerId
         *          Ledger ID.
         * @return true if and only the entries of the ledger should be scanned.
         */
        public boolean accept(long ledgerId);

        /**
         * Process an entry.
         *
         * @param ledgerId
         *          Ledger ID.
         * @param offset
         *          File offset of this entry.
         * @param entry
         *          Entry ByteBuf
         * @throws IOException
         */
        public void process(long ledgerId, long offset, ByteBuf entry) throws IOException;
    }

    /**
     * Entry Log Listener
     */
    static interface EntryLogListener {
        /**
         * Rotate a new entry log to write.
         */
        public void onRotateEntryLog();
    }

    /**
     * Create an EntryLogger that stores it's log files in the given
     * directories
     */
    public EntryLogger(ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager) throws IOException {
        this(conf, ledgerDirsManager, null);
    }

    public EntryLogger(ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager, EntryLogListener listener)
                    throws IOException {
        this.ledgerDirsManager = ledgerDirsManager;
        if (listener != null) {
            addListener(listener);
        }
        // log size limit
        this.logSizeLimit = conf.getEntryLogSizeLimit();
        this.entryLogPreAllocationEnabled = conf.isEntryLogFilePreAllocationEnabled();

        // Initialize the entry log header buffer. This cannot be a static object
        // since in our unit tests, we run multiple Bookies and thus EntryLoggers
        // within the same JVM. All of these Bookie instances access this header
        // so there can be race conditions when entry logs are rolled over and
        // this header buffer is cleared before writing it into the new logChannel.
        LOGFILE_HEADER.writeBytes("BKLO".getBytes(UTF_8));
        LOGFILE_HEADER.writeInt(HeaderVersion.V1.value);
        LOGFILE_HEADER.writerIndex(LOGFILE_HEADER_SIZE);

        // Find the largest logId
        long logId = INVALID_LID;
        for (File dir : ledgerDirsManager.getAllLedgerDirs()) {
            if (!dir.exists()) {
                throw new FileNotFoundException(
                        "Entry log directory does not exist");
            }
            long lastLogId = getLastLogId(dir);
            if (lastLogId > logId) {
                logId = lastLogId;
            }
        }
        this.leastUnflushedLogId = logId + 1;
        this.entryLoggerAllocator = new EntryLoggerAllocator(logId);
        this.conf = conf;
        initialize();
    }

    void addListener(EntryLogListener listener) {
        if (null != listener) {
            listeners.add(listener);
        }
    }

    /**
     * If the log id of current writable channel is the same as entryLogId and the position
     * we want to read might end up reading from a position in the write buffer of the
     * buffered channel, route this read to the current logChannel. Else,
     * read from the BufferedReadChannel that is provided.
     * @param entryLogId
     * @param channel
     * @param buff remaining() on this bytebuffer tells us the last position that we
     *             expect to read.
     * @param pos The starting position from where we want to read.
     * @return
     */
    private int readFromLogChannel(long entryLogId, BufferedReadChannel channel, ByteBuf buff, long pos)
            throws IOException {
        BufferedLogChannel bc = logChannel;
        if (null != bc) {
            if (entryLogId == bc.getLogId()) {
                synchronized (bc) {
                    if (pos + buff.writableBytes() >= bc.getFileChannelPosition()) {
                        return bc.read(buff, pos);
                    }
                }
            }
        }
        return channel.read(buff, pos);
    }

    /**
     * A thread-local variable that wraps a mapping of log ids to bufferedchannels
     * These channels should be used only for reading. logChannel is the one
     * that is used for writes.
     */
    private final ThreadLocal<Map<Long, BufferedReadChannel>> logid2Channel
            = new ThreadLocal<Map<Long, BufferedReadChannel>>() {
        @Override
        public Map<Long, BufferedReadChannel> initialValue() {
            // Since this is thread local there only one modifier
            // We dont really need the concurrency, but we need to use
            // the weak values. Therefore using the concurrency level of 1
            return new MapMaker().concurrencyLevel(1)
                .weakValues()
                .makeMap();
        }
    };

    /**
     * Each thread local buffered read channel can share the same file handle because reads are not relative
     * and don't cause a change in the channel's position. We use this map to store the file channels. Each
     * file channel is mapped to a log id which represents an open log file.
     */
    private final ConcurrentMap<Long, FileChannel> logid2FileChannel
            = new ConcurrentHashMap<Long, FileChannel>();

    /**
     * Put the logId, bc pair in the map responsible for the current thread.
     * @param logId
     * @param bc
     */
    public BufferedReadChannel putInReadChannels(long logId, BufferedReadChannel bc) {
        Map<Long, BufferedReadChannel> threadMap = logid2Channel.get();
        return threadMap.put(logId, bc);
    }

    /**
     * Remove all entries for this log file in each thread's cache.
     * @param logId
     */
    public void removeFromChannelsAndClose(long logId) {
        FileChannel fileChannel = logid2FileChannel.remove(logId);
        if (null != fileChannel) {
            try {
                fileChannel.close();
            } catch (IOException e) {
                LOG.warn("Exception while closing channel for log file:" + logId);
            }
        }
    }

    public BufferedReadChannel getFromChannels(long logId) {
        return logid2Channel.get().get(logId);
    }

    /**
     * Get the least unflushed log id. Garbage collector thread should not process
     * unflushed entry log file.
     *
     * @return least unflushed log id.
     */
    synchronized long getLeastUnflushedLogId() {
        return leastUnflushedLogId;
    }

    synchronized long getCurrentLogId() {
        return logChannel.getLogId();
    }

    protected void initialize() throws IOException {
        // Register listener for disk full notifications.
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        // create a new log to write
        createNewLog();
    }

    private LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsListener() {
            @Override
            public void diskFull(File disk) {
                // If the current entry log disk is full, then create new entry
                // log.
                if (currentDir != null && currentDir.equals(disk)) {
                    shouldCreateNewEntryLog.set(true);
                }
            }

            @Override
            public void diskAlmostFull(File disk) {
                // If the current entry log disk is almost full, then create new entry
                // log.
                if (currentDir != null && currentDir.equals(disk)) {
                    shouldCreateNewEntryLog.set(true);
                }
            }

            @Override
            public void diskFailed(File disk) {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void allDisksFull() {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void fatalError() {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void diskWritable(File disk) {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void diskJustWritable(File disk) {
                // Nothing to handle here. Will be handled in Bookie
            }
        };
    }

    /**
     * Rolling a new log file to write.
     */
    synchronized void rollLog() throws IOException {
        createNewLog();
    }

    /**
     * Creates a new log file
     */
    void createNewLog() throws IOException {
        // first tried to create a new log channel. add current log channel to ToFlush list only when
        // there is a new log channel. it would prevent that a log channel is referenced by both
        // *logChannel* and *ToFlush* list.
        if (null != logChannel) {
            if (null == logChannelsToFlush) {
                logChannelsToFlush = new LinkedList<BufferedLogChannel>();
            }

            // flush the internal buffer back to filesystem but not sync disk
            // so the readers could access the data from filesystem.
            logChannel.flush(false);

            // Append ledgers map at the end of entry log
            appendLedgersMap(logChannel);

            BufferedLogChannel newLogChannel = entryLoggerAllocator.createNewLog();
            logChannelsToFlush.add(logChannel);
            LOG.info("Flushing entry logger {} back to filesystem, pending for syncing entry loggers : {}.",
                    logChannel.getLogId(), logChannelsToFlush);
            for (EntryLogListener listener : listeners) {
                listener.onRotateEntryLog();
            }
            logChannel = newLogChannel;
        } else {
            logChannel = entryLoggerAllocator.createNewLog();
        }
    }

    /**
     * Append the ledger map at the end of the entry log.
     * Updates the entry log file header with the offset and size of the map.
     */
    private void appendLedgersMap(BufferedLogChannel entryLogChannel) throws IOException {
        long ledgerMapOffset = entryLogChannel.position();

        ConcurrentLongLongHashMap ledgersMap = entryLogChannel.getLedgersMap();
        int numberOfLedgers = (int) ledgersMap.size();

        // Write the ledgers map into several batches
        final int maxMapSize = LEDGERS_MAP_HEADER_SIZE + LEDGERS_MAP_ENTRY_SIZE * LEDGERS_MAP_MAX_BATCH_SIZE;
        final ByteBuf serializedMap = ByteBufAllocator.DEFAULT.buffer(maxMapSize);

        try {
            ledgersMap.forEach(new BiConsumerLong() {
                int remainingLedgers = numberOfLedgers;
                boolean startNewBatch = true;
                int remainingInBatch = 0;

                @Override
                public void accept(long ledgerId, long size) {
                    if (startNewBatch) {
                        int batchSize = Math.min(remainingLedgers, LEDGERS_MAP_MAX_BATCH_SIZE);
                        int ledgerMapSize = LEDGERS_MAP_HEADER_SIZE + LEDGERS_MAP_ENTRY_SIZE * batchSize;

                        serializedMap.clear();
                        serializedMap.writeInt(ledgerMapSize - 4);
                        serializedMap.writeLong(INVALID_LID);
                        serializedMap.writeLong(LEDGERS_MAP_ENTRY_ID);
                        serializedMap.writeInt(batchSize);

                        startNewBatch = false;
                        remainingInBatch = batchSize;
                    }
                    // Dump the ledger in the current batch
                    serializedMap.writeLong(ledgerId);
                    serializedMap.writeLong(size);
                    --remainingLedgers;

                    if (--remainingInBatch == 0) {
                        // Close current batch
                        try {
                            entryLogChannel.write(serializedMap);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        startNewBatch = true;
                    }
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw e;
            }
        } finally {
            serializedMap.release();
        }
        //Flush the ledger's map out before we write the header.  Otherwise the header might point to something that is not fully written
        entryLogChannel.flush(false);

        // Update the headers with the map offset and count of ledgers
        ByteBuffer mapInfo = ByteBuffer.allocate(8 + 4);
        mapInfo.putLong(ledgerMapOffset);
        mapInfo.putInt(numberOfLedgers);
        mapInfo.flip();
        entryLogChannel.fileChannel.write(mapInfo, LEDGERS_MAP_OFFSET_POSITION);
    }

    /**
     * An allocator pre-allocates entry log files.
     */
    class EntryLoggerAllocator {

        long preallocatedLogId;
        Future<BufferedLogChannel> preallocation = null;
        ExecutorService allocatorExecutor;

        EntryLoggerAllocator(long logId) {
            preallocatedLogId = logId;
            allocatorExecutor = Executors.newSingleThreadExecutor();
        }

        synchronized BufferedLogChannel createNewLog() throws IOException {
            BufferedLogChannel bc;
            if (!entryLogPreAllocationEnabled || null == preallocation) {
                // initialization time to create a new log
                bc = allocateNewLog();
            } else {
                // has a preallocated entry log
                try {
                    bc = preallocation.get();
                } catch (ExecutionException ee) {
                    if (ee.getCause() instanceof IOException) {
                        throw (IOException) (ee.getCause());
                    } else {
                        throw new IOException("Error to execute entry log allocation.", ee);
                    }
                } catch (CancellationException ce) {
                    throw new IOException("Task to allocate a new entry log is cancelled.", ce);
                } catch (InterruptedException ie) {
                    throw new IOException("Intrrupted when waiting a new entry log to be allocated.", ie);
                }
                preallocation = allocatorExecutor.submit(new Callable<BufferedLogChannel>() {
                    @Override
                    public BufferedLogChannel call() throws IOException {
                        return allocateNewLog();
                    }
                });
            }
            LOG.info("Created new entry logger {}.", bc.getLogId());
            return bc;
        }

        /**
         * Allocate a new log file.
         */
        BufferedLogChannel allocateNewLog() throws IOException {
            List<File> list = ledgerDirsManager.getWritableLedgerDirs();
            Collections.shuffle(list);
            // It would better not to overwrite existing entry log files
            File newLogFile = null;
            do {
                String logFileName = Long.toHexString(++preallocatedLogId) + ".log";
                for (File dir : list) {
                    newLogFile = new File(dir, logFileName);
                    currentDir = dir;
                    if (newLogFile.exists()) {
                        LOG.warn("Found existed entry log " + newLogFile
                               + " when trying to create it as a new log.");
                        newLogFile = null;
                        break;
                    }
                }
            } while (newLogFile == null);

            FileChannel channel = new RandomAccessFile(newLogFile, "rw").getChannel();
            BufferedLogChannel logChannel = new BufferedLogChannel(channel,
                    conf.getWriteBufferBytes(), conf.getReadBufferBytes(), preallocatedLogId);
            LOGFILE_HEADER.readerIndex(0);
            logChannel.write(LOGFILE_HEADER);

            for (File f : list) {
                setLastLogId(f, preallocatedLogId);
            }
            LOG.info("Preallocated entry logger {}.", preallocatedLogId);
            return logChannel;
        }

        /**
         * Stop the allocator.
         */
        void stop() {
            // wait until the preallocation finished.
            allocatorExecutor.shutdown();
            LOG.info("Stopped entry logger preallocator.");
        }
    }

    /**
     * Remove entry log.
     *
     * @param entryLogId
     *          Entry Log File Id
     */
    protected boolean removeEntryLog(long entryLogId) {
        removeFromChannelsAndClose(entryLogId);
        File entryLogFile;
        try {
            entryLogFile = findFile(entryLogId);
        } catch (FileNotFoundException e) {
            LOG.error("Trying to delete an entryLog file that could not be found: "
                    + entryLogId + ".log");
            return false;
        }
        if (!entryLogFile.delete()) {
            LOG.warn("Could not delete entry log file {}", entryLogFile);
        }
        return true;
    }

    /**
     * writes the given id to the "lastId" file in the given directory.
     */
    private void setLastLogId(File dir, long logId) throws IOException {
        FileOutputStream fos;
        fos = new FileOutputStream(new File(dir, "lastId"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, UTF_8));
        try {
            bw.write(Long.toHexString(logId) + "\n");
            bw.flush();
        } finally {
            try {
                bw.close();
            } catch (IOException e) {
                LOG.error("Could not close lastId file in {}", dir.getPath());
            }
        }
    }

    private long getLastLogId(File dir) {
        long id = readLastLogId(dir);
        // read success
        if (id > 0) {
            return id;
        }
        // read failed, scan the ledger directories to find biggest log id
        File[] logFiles = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.getName().endsWith(".log");
            }
        });
        List<Long> logs = new ArrayList<Long>();
        for (File lf : logFiles) {
            String idString = lf.getName().split("\\.")[0];
            try {
                long lid = Long.parseLong(idString, 16);
                logs.add(lid);
            } catch (NumberFormatException nfe) {
            }
        }
        // no log file found in this directory
        if (0 == logs.size()) {
            return INVALID_LID;
        }
        // order the collections
        Collections.sort(logs);
        return logs.get(logs.size() - 1);
    }

    /**
     * reads id from the "lastId" file in the given directory.
     */
    private long readLastLogId(File f) {
        FileInputStream fis;
        try {
            fis = new FileInputStream(new File(f, "lastId"));
        } catch (FileNotFoundException e) {
            return INVALID_LID;
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, UTF_8));
        try {
            String lastIdString = br.readLine();
            return Long.parseLong(lastIdString, 16);
        } catch (IOException e) {
            return INVALID_LID;
        } catch(NumberFormatException e) {
            return INVALID_LID;
        } finally {
            try {
                br.close();
            } catch (IOException e) {
            }
        }
    }

    /**
     * Flushes all rotated log channels. After log channels are flushed,
     * move leastUnflushedLogId ptr to current logId.
     */
    void checkpoint() throws IOException {
        flushRotatedLogs();
    }

    void flushRotatedLogs() throws IOException {
        List<BufferedLogChannel> channels = null;
        long flushedLogId = INVALID_LID;
        synchronized (this) {
            channels = logChannelsToFlush;
            logChannelsToFlush = null;
        }
        if (null == channels) {
            return;
        }
        for (BufferedLogChannel channel : channels) {
            channel.flush(true);
            // since this channel is only used for writing, after flushing the channel,
            // we had to close the underlying file channel. Otherwise, we might end up
            // leaking fds which cause the disk spaces could not be reclaimed.
            closeFileChannel(channel);
            if (channel.getLogId() > flushedLogId) {
                flushedLogId = channel.getLogId();
            }
            LOG.info("Synced entry logger {} to disk.", channel.getLogId());
        }
        // move the leastUnflushedLogId ptr
        leastUnflushedLogId = flushedLogId + 1;
    }

    public synchronized void flush() throws IOException {
        flushRotatedLogs();
        flushCurrentLog();
    }

    synchronized void flushCurrentLog() throws IOException {
        if (logChannel != null) {
            logChannel.flush(true);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Flush and sync current entry logger {}.", logChannel.getLogId());
            }
        }
    }

    long addEntry(long ledger, ByteBuffer entry) throws IOException {
        return addEntry(ledger, Unpooled.wrappedBuffer(entry), true);
    }

    long addEntry(long ledger, ByteBuf entry) throws IOException {
        return addEntry(ledger, entry, true);
    }


    private final FastThreadLocal<ByteBuf> sizeBuffer = new FastThreadLocal<ByteBuf>() {
        @Override
        protected ByteBuf initialValue() throws Exception {
            return Unpooled.buffer(4);
        }
    };

    public synchronized long addEntry(long ledger, ByteBuf entry, boolean rollLog)
            throws IOException {
        int entrySize = entry.readableBytes() + 4; // Adding 4 bytes to prepend the size

        if (rollLog) {
            // Create new log if logSizeLimit reached or current disk is full
            boolean createNewLog = shouldCreateNewEntryLog.get();
            if (createNewLog || reachEntryLogLimit(entrySize)) {
                createNewLog();
                // Reset the flag
                if (createNewLog) {
                    shouldCreateNewEntryLog.set(false);
                }
            }
        }

        // Get a buffer from recyclable pool to store the size
        ByteBuf sizeBuffer = this.sizeBuffer.get();
        sizeBuffer.clear();
        sizeBuffer.writeInt(entry.readableBytes());
        logChannel.write(sizeBuffer);

        long pos = logChannel.position();
        logChannel.write(entry);
        logChannel.registerWrittenEntry(ledger, entrySize);

        return (logChannel.getLogId() << 32L) | pos;
    }

    static long logIdForOffset(long offset) {
        return offset >> 32L;
    }

    synchronized boolean reachEntryLogLimit(long size) {
        return logChannel.position() + size > logSizeLimit;
    }

    public ByteBuf internalReadEntry(long ledgerId, long entryId, long location) throws IOException, Bookie.NoEntryException {
        long entryLogId = logIdForOffset(location);
        long pos = location & 0xffffffffL;
        ByteBuf sizeBuff = sizeBuffer.get();
        sizeBuff.clear();
        pos -= 4; // we want to get the ledgerId and length to check
        BufferedReadChannel fc;
        try {
            fc = getChannelForLogId(entryLogId);
        } catch (FileNotFoundException e) {
            FileNotFoundException newe = new FileNotFoundException(e.getMessage() + " for " + ledgerId + " with location " + location);
            newe.setStackTrace(e.getStackTrace());
            throw newe;
        }
        if (readFromLogChannel(entryLogId, fc, sizeBuff, pos) != sizeBuff.capacity()) {
            throw new Bookie.NoEntryException("Short read from entrylog " + entryLogId,
                                              ledgerId, entryId);
        }
        pos += 4;
        int entrySize = sizeBuff.readInt();

        // entrySize does not include the ledgerId
        if (entrySize > MAX_SANE_ENTRY_SIZE) {
            LOG.warn("Sanity check failed for entry size of " + entrySize + " at location " + pos + " in " + entryLogId);
        }
        if (entrySize < MIN_SANE_ENTRY_SIZE) {
            LOG.error("Read invalid entry length {}", entrySize);
            throw new IOException("Invalid entry length " + entrySize);
        }

        ByteBuf data = PooledByteBufAllocator.DEFAULT.directBuffer(entrySize, entrySize);
        int rc = readFromLogChannel(entryLogId, fc, data, pos);
        if (rc != entrySize) {
            // Note that throwing NoEntryException here instead of IOException is not
            // without risk. If all bookies in a quorum throw this same exception
            // the client will assume that it has reached the end of the ledger.
            // However, this may not be the case, as a very specific error condition
            // could have occurred, where the length of the entry was corrupted on all
            // replicas. However, the chance of this happening is very very low, so
            // returning NoEntryException is mostly safe.
            data.release();
            throw new Bookie.NoEntryException("Short read for " + ledgerId + "@"
                                              + entryId + " in " + entryLogId + "@"
                                              + pos + "("+rc+"!="+entrySize+")", ledgerId, entryId);
        }

        return data;
    }

    public ByteBuf readEntry(long ledgerId, long entryId, long location) throws IOException, Bookie.NoEntryException {
        long entryLogId = logIdForOffset(location);
        long pos = location & 0xffffffffL;

        ByteBuf data = internalReadEntry(ledgerId, entryId, location);
        long thisLedgerId = data.getLong(0);
        if (thisLedgerId != ledgerId) {
            data.release();
            throw new IOException("problem found in " + entryLogId + "@" + entryId + " at position + " + pos + " entry belongs to " + thisLedgerId + " not " + ledgerId);
        }
        long thisEntryId = data.getLong(8);
        if (thisEntryId != entryId) {
            data.release();
            throw new IOException("problem found in " + entryLogId + "@" + entryId + " at position + " + pos + " entry is " + thisEntryId + " not " + entryId);
        }

        return data;
    }

    /**
     * Read the header of an entry log
     */
    private Header getHeaderForLogId(long entryLogId) throws IOException {
        BufferedReadChannel bc = getChannelForLogId(entryLogId);

        // Allocate buffer to read (version, ledgersMapOffset, ledgerCount)
        ByteBuf headers = PooledByteBufAllocator.DEFAULT.directBuffer(LOGFILE_HEADER_SIZE);
        try {
            bc.read(headers, 0);

            if (headers.readableBytes() < LOGFILE_HEADER_SIZE) {
                // This might occur when the bookie crashed while entry log file was created
                throw new IOException("Could not find header in the entry log file");
            }

            // Skip marker string "BKLO"
            headers.readInt();

            int version = headers.readInt();
            HeaderVersion headerVersion;
            switch (version) {
            case 0:
                headerVersion = HeaderVersion.V0;
                break;
            case 1:
                headerVersion = HeaderVersion.V1;
                break;
            default:
                LOG.debug("Unknown entry log header version for log {}: {}", entryLogId, version);
                headerVersion = HeaderVersion.Unknown;
                break;
            }

            long ledgersMapOffset = headers.readLong();
            int ledgersCount = headers.readInt();
            return new Header(headerVersion, ledgersMapOffset, ledgersCount);
        } finally {
            headers.release();
        }
    }

    private BufferedReadChannel getChannelForLogId(long entryLogId) throws IOException {
        BufferedReadChannel fc = getFromChannels(entryLogId);
        if (fc != null) {
            return fc;
        }
        File file = findFile(entryLogId);
        // get channel is used to open an existing entry log file
        // it would be better to open using read mode
        FileChannel newFc = new RandomAccessFile(file, "r").getChannel();
        FileChannel oldFc = logid2FileChannel.putIfAbsent(entryLogId, newFc);
        if (null != oldFc) {
            newFc.close();
            newFc = oldFc;
        }

        // We set the position of the write buffer of this buffered channel to Long.MAX_VALUE
        // so that there are no overlaps with the write buffer while reading
        fc = new BufferedReadChannel(newFc, conf.getReadBufferBytes());
        putInReadChannels(entryLogId, fc);
        return fc;
    }

    /**
     * Whether the log file exists or not.
     */
    boolean logExists(long logId) {
        for (File d : ledgerDirsManager.getAllLedgerDirs()) {
            File f = new File(d, Long.toHexString(logId) + ".log");
            if (f.exists()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a set with the ids of all the entry log files
     * @throws IOException
     */
    public Set<Long> getEntryLogsSet() throws IOException {
        Set<Long> entryLogs = Sets.newTreeSet();

        final FilenameFilter logFileFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".log");
            }
        };

        for (File d : ledgerDirsManager.getAllLedgerDirs()) {
            for (File f : d.listFiles(logFileFilter)) {
                Long entryLogId = Long.parseLong(f.getName().split(".log")[0], 16);
                entryLogs.add(entryLogId);
            }
        }
        return entryLogs;
    }

    private File findFile(long logId) throws FileNotFoundException {
        for (File d : ledgerDirsManager.getAllLedgerDirs()) {
            File f = new File(d, Long.toHexString(logId)+".log");
            if (f.exists()) {
                return f;
            }
        }
        throw new FileNotFoundException("No file for log " + Long.toHexString(logId));
    }

    /**
     * Scan entry log
     *
     * @param entryLogId
     *          Entry Log Id
     * @param scanner
     *          Entry Log Scanner
     * @throws IOException
     */
    public void scanEntryLog(long entryLogId, EntryLogScanner scanner) throws IOException {
        // Buffer where to read the entrySize (4 bytes) and the ledgerId (8 bytes)
        ByteBuf headerBuffer = Unpooled.buffer(4 + 8);
        BufferedReadChannel bc;
        // Get the BufferedChannel for the current entry log file
        try {
            bc = getChannelForLogId(entryLogId);
        } catch (IOException e) {
            LOG.warn("Failed to get channel to scan entry log: " + entryLogId + ".log");
            throw e;
        }
        // Start the read position in the current entry log file to be after
        // the header where all of the ledger entries are.
        long pos = LOGFILE_HEADER_SIZE;

        // Start with a reasonably sized buffer size
        ByteBuf data = PooledByteBufAllocator.DEFAULT.directBuffer(1024 * 1024);

        try {

            // Read through the entry log file and extract the ledger ID's.
            while (true) {
                // Check if we've finished reading the entry log file.
                if (pos >= bc.size()) {
                    break;
                }
                if (readFromLogChannel(entryLogId, bc, headerBuffer, pos) != headerBuffer.capacity()) {
                    LOG.warn("Short read for entry size from entrylog {}", entryLogId);
                    return;
                }
                long offset = pos;
                pos += 4;
                int entrySize = headerBuffer.readInt();
                long ledgerId = headerBuffer.readLong();
                headerBuffer.clear();

                if (ledgerId == INVALID_LID || !scanner.accept(ledgerId)) {
                    // skip this entry
                    pos += entrySize;
                    continue;
                }
                // read the entry

                data.clear();
                data.capacity(entrySize);
                int rc = readFromLogChannel(entryLogId, bc, data, pos);
                if (rc != entrySize) {
                    LOG.warn("Short read for ledger entry from entryLog {}@{} ({} != {})",
                            new Object[] { entryLogId, pos, rc, entrySize });
                    return;
                }
                // process the entry
                scanner.process(ledgerId, offset, data);

                // Advance position to the next entry
                pos += entrySize;
            }
        } finally {
            data.release();
        }
    }

    public EntryLogMetadata getEntryLogMetadata(long entryLogId) throws IOException {
        // First try to extract the EntryLogMetada from the index, if there's no index then fallback to scanning the
        // entry log
        try {
            return extractEntryLogMetadataFromIndex(entryLogId);
        } catch (IOException e) {
            LOG.info("Failed to get ledgers map index from: {}.log : {}", entryLogId, e.getMessage());

            // Fall-back to scanning
            return extractEntryLogMetadataByScanning(entryLogId);
        }
    }

    EntryLogMetadata extractEntryLogMetadataFromIndex(long entryLogId) throws IOException {
        Header header = getHeaderForLogId(entryLogId);

        if (header.version.value < HeaderVersion.V1.value) {
            throw new IOException("Old log file header");
        }

        if (header.ledgersMapOffset == 0L) {
            // The index was not stored in the log file (possibly because the bookie crashed before flushing it)
            throw new IOException("No ledgers map index found");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Recovering ledgers maps for log {} at offset: {}", entryLogId, header.ledgersMapOffset);
        }

        BufferedReadChannel bc = getChannelForLogId(entryLogId);

        // There can be multiple entries containing the various components of the serialized ledgers map
        long offset = header.ledgersMapOffset;
        EntryLogMetadata meta = new EntryLogMetadata(entryLogId);
        ByteBuf sizeBuf = ByteBufAllocator.DEFAULT.directBuffer(4);

        final int maxMapSize = LEDGERS_MAP_HEADER_SIZE + LEDGERS_MAP_ENTRY_SIZE * LEDGERS_MAP_MAX_BATCH_SIZE;
        ByteBuf ledgersMap = ByteBufAllocator.DEFAULT.directBuffer(maxMapSize);

        try {
            while (offset < bc.size()) {
                // Read ledgers map size
                sizeBuf.clear();
                bc.read(sizeBuf, offset);

                int ledgersMapSize = sizeBuf.readInt();

                // Read the index into a buffer
                ledgersMap.clear();
                bc.read(ledgersMap, offset + 4, ledgersMapSize);

                // Discard ledgerId and entryId
                long lid = ledgersMap.readLong();
                if (lid != INVALID_LID) {
                    throw new IOException("Cannot deserialize ledgers map from ledger " + lid);
                }

                long entryId = ledgersMap.readLong();
                if (entryId != LEDGERS_MAP_ENTRY_ID) {
                    throw new IOException("Cannot deserialize ledgers map from entryId " + entryId);
                }

                // Read the number of ledgers in the current entry batch
                int ledgersCount = ledgersMap.readInt();

                // Extract all (ledger,size) tuples from buffer
                for (int i = 0; i < ledgersCount; i++) {
                    long ledgerId = ledgersMap.readLong();
                    long size = ledgersMap.readLong();

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Recovering ledgers maps for log {} -- Found ledger: {} with size: {}",
                                new Object[] { entryLogId, ledgerId, size });
                    }
                    meta.addLedgerSize(ledgerId, size);
                }

                if (ledgersMap.isReadable()) {
                    throw new IOException("Invalid entry size when reading ledgers map");
                }

                // Move to next entry, if any
                offset += ledgersMapSize + 4;
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        } finally {
            sizeBuf.release();
            ledgersMap.release();
        }

        if (meta.getLedgersMap().size() != header.ledgersCount) {
            throw new IOException("Not all ledgers were found in ledgers map index. expected: " + header.ledgersCount
                    + " -- found: " + meta.getLedgersMap().size());
        }

        return meta;
    }

    private EntryLogMetadata extractEntryLogMetadataByScanning(long entryLogId) throws IOException {
        final EntryLogMetadata meta = new EntryLogMetadata(entryLogId);

        // Read through the entry log file and extract the entry log meta
        scanEntryLog(entryLogId, new EntryLogScanner() {
            @Override
            public void process(long ledgerId, long offset, ByteBuf entry) throws IOException {
                // add new entry size of a ledger to entry log meta
                meta.addLedgerSize(ledgerId, entry.readableBytes() + 4);
            }

            @Override
            public boolean accept(long ledgerId) {
                return ledgerId > 0;
            }
        });

        if (LOG.isDebugEnabled()) {
            LOG.debug("Retrieved entry log meta data entryLogId: {}, meta: {}", entryLogId, meta);
        }
        return meta;
    }

    /**
     * Shutdown method to gracefully stop entry logger.
     */
    public void shutdown() {
        // since logChannel is buffered channel, do flush when shutting down
        LOG.info("Stopping EntryLogger");
        try {
            flush();
            for (FileChannel fc : logid2FileChannel.values()) {
                fc.close();
            }
            // clear the mapping, so we don't need to go through the channels again in finally block in normal case.
            logid2FileChannel.clear();
            // close current writing log file
            closeFileChannel(logChannel);
            logChannel = null;
        } catch (IOException ie) {
            // we have no idea how to avoid io exception during shutting down, so just ignore it
            LOG.error("Error flush entry log during shutting down, which may cause entry log corrupted.", ie);
        } finally {
            for (FileChannel fc : logid2FileChannel.values()) {
                IOUtils.close(LOG, fc);
            }
            forceCloseFileChannel(logChannel);
        }
        // shutdown the pre-allocation thread
        entryLoggerAllocator.stop();
    }

    private static void closeFileChannel(BufferedChannelBase channel) throws IOException {
        if (null == channel) {
            return;
        }
        channel.close();
        FileChannel fileChannel = channel.getFileChannel();
        if (null != fileChannel) {
            fileChannel.close();
        }
    }

    private static void forceCloseFileChannel(BufferedChannelBase channel) {
        if (null == channel) {
            return;
        }
        FileChannel fileChannel = channel.getFileChannel();
        if (null != fileChannel) {
            IOUtils.close(LOG, fileChannel);
        }
    }

}
