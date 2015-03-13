/*
 *  Copyright 2015 Brian S O'Neill
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import java.io.IOException;

import java.util.Arrays;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class Fetcher extends WorkerPool.Task {
    // FIXME: testing
    public static void main(String[] args) throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .checkpointSizeThreshold(0)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .minCacheSize(500_000_000L) // 100_000_000L if smaller page size
            ;

        if (args.length > 0) {
            config.baseFilePath(args[0]);
            if (args.length > 1) {
                java.io.File[] files = new java.io.File[args.length - 1];
                for (int i=1; i<args.length; i++) {
                    files[i - 1] = new java.io.File(args[i]);
                }
                config.dataFiles(files);
            }
        }
 
        Database db = Database.open(config);
        Index ix = db.findIndex("ix0");

        WorkerPool pool = new WorkerPool(10);
        pool.init();

        Cursor c = ix.newCursor(Transaction.BOGUS);
        Fetcher fetcher = new Fetcher(pool, (TreeCursor) c, 1000);
        fetcher.init();

        byte[][] entries[] = new byte[2][][];

        int total = 0;
        while (true) {
            int count = fetcher.nextBatch(entries);
            System.out.println(count);
            byte[][] keys = entries[0];
            if (keys[0] == null) {
                break;
            }
            System.out.println(Utils.toHex(keys[0]));
            total += count;
        }

        pool.close();

        System.out.println(total);
    }

    private final WorkerPool mPool;
    private final TreeCursor mSource;

    private byte[][] mKeys;
    private byte[][] mValues;

    private int mEntryCount;

    private byte[][] mNextKeys;
    private byte[][] mNextValues;

    private Throwable mException;

    Fetcher(WorkerPool pool, TreeCursor source, int capacity) {
        mPool = pool;
        mSource = source;

        mKeys = new byte[capacity][];
        mValues = new byte[capacity][];
        mNextKeys = new byte[capacity][];
        mNextValues = new byte[capacity][];
    }

    public synchronized void init() throws IOException {
        Arrays.fill(mKeys, null);
        Arrays.fill(mValues, null);
        Arrays.fill(mNextKeys, null);
        Arrays.fill(mNextValues, null);

        mException = null;
        mEntryCount = 0;
        mSource.first();
        if (mSource.key() == null) {
            mEntryCount = 1;
        } else {
            mPool.enqueue(this);
        }
    }

    /**
     * @param entries element 0 receives the keys, element 1 receives the values
     */
    public synchronized int nextBatch(byte[][] entries[]) {
        int count;
        while ((count = mEntryCount) == 0) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }

        byte[][] keys = mKeys;
        byte[][] values = mValues;

        entries[0] = keys;
        entries[1] = values;

        mKeys = mNextKeys;
        mValues = mNextValues;

        mNextKeys = keys;
        mNextValues = values;

        mEntryCount = 0;

        if (keys[0] != null) {
            mPool.enqueue(this);
        }

        return count;
    }

    public synchronized void close() {
        mSource.reset();
    }

    public synchronized Throwable exceptionCheck() {
        return mException;
    }

    protected void advance(TreeCursor c) throws IOException {
        c.next();
    }

    @Override
    protected boolean runTask() {
        byte[][] keys = mKeys;
        byte[][] values = mValues;

        int i = 0;
        TreeCursor c = mSource;

        try {
            do {
                values[i] = c.value();
                byte[] key = c.key();
                keys[i++] = key;
                if (key == null) {
                    break;
                }
                advance(c);
            } while (i < keys.length);
        } catch (Throwable e) {
            synchronized (this) {
                if (mException == null) {
                    e.printStackTrace();
                    mException = e;
                }
            }
            values[i] = null;
            keys[i++] = null;
        }

        synchronized (this) {
            mEntryCount = i;
            notify();
        }

        return true;
    }
}
