package org.apache.bookkeeper.bookie.storage.ldb;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DbLedgerStorageWriteCacheTest {

    private DbLedgerStorage storage;
    private File tmpDir;

    private static class MockedDbLedgerStorage extends DbLedgerStorage {

        @Override
        public synchronized void flush() throws IOException {
            // Swap the write caches and block indefinitely to simulate a slow disk
            WriteCache tmp = writeCacheBeingFlushed;
            writeCacheBeingFlushed = writeCache;
            writeCache = tmp;

            // since the cache is switched, we can allow flush to be triggered
            hasFlushBeenTriggered.set(false);

            // Block the flushing thread
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

    }

    @Before
    public void setup() throws Exception {
        tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = new ServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setAllowLoopback(true);
        conf.setLedgerStorageClass(MockedDbLedgerStorage.class.getName());
        conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 1);
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        Bookie bookie = new Bookie(conf);

        storage = (DbLedgerStorage) bookie.getLedgerStorage();
    }

    @After
    public void teardown() throws Exception {
        tmpDir.delete();
    }

    @Test
    public void writeCacheFull() throws Exception {
        storage.setMasterKey(4, "key".getBytes());
        assertEquals(false, storage.isFenced(4));
        assertEquals(true, storage.ledgerExists(4));

        assertEquals("key", new String(storage.readMasterKey(4)));

        // Add enough entries to fill the 1st write cache
        for (int i = 0; i < 5; i++) {
            ByteBuf entry = Unpooled.buffer(100 * 1024 + 2 * 8);
            entry.writeLong(4); // ledger id
            entry.writeLong(i); // entry id
            entry.writeZero(100 * 1024);
            storage.addEntry(entry);
        }

        for (int i = 0; i < 5; i++) {
            ByteBuf entry = Unpooled.buffer(100 * 1024 + 2 * 8);
            entry.writeLong(4); // ledger id
            entry.writeLong(5 + i); // entry id
            entry.writeZero(100 * 1024);
            storage.addEntry(entry);
        }

        // Next add should fail for cache full
        ByteBuf entry = Unpooled.buffer(100 * 1024 + 2 * 8);
        entry.writeLong(4); // ledger id
        entry.writeLong(22); // entry id
        entry.writeZero(100 * 1024);

        try {
            storage.addEntry(entry);
            fail("Should have thrown exception");
        } catch (IOException e) {
            // Expected
        }
    }
}