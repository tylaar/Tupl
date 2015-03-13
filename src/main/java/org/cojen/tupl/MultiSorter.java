/*
 *  Copyright 2014 Brian S O'Neill
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

import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class MultiSorter {
    /**
     * Testing.
     */
    public static void main(String[] args) throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .checkpointSizeThreshold(0)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            //.checkpointRate(-1, null)
            .minCacheSize(500_000_000L) // 100_000_000L if smaller page size
            //.pageSize(65536)
            //.eventListener(new EventPrinter())
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

        if (true) {
            MultiSorter s = new MultiSorter(db);

            byte[] key = new byte[5];
            byte[] value = new byte[0];
            java.util.Random rnd = new java.util.Random(8926489);

            final int count = 2_000_000_000;
            for (int i=0; i<count; i++) {
                if (i % 100_000 == 0) {
                    System.out.println(i);
                }
                rnd.nextBytes(key);
                s.add(key, value);
                key = key.clone();
            }

            Index ix = s.finish();
        }

        db.checkpoint();
    }

    private final Sorter[] mSubSorters;

    private final Worker[] mWorkers;
    private final BlockingQueue<Worker> mWorkerPool;

    private Worker mBatch;

    public MultiSorter(Database db) {
        mSubSorters = new Sorter[8]; // FIXME: configurable
        for (int i=0; i<mSubSorters.length; i++) {
            mSubSorters[i] = new Sorter(db, 1_000_000, 100, "" + i); // FIXME: configurable
        }

        mWorkers = new Worker[mSubSorters.length];
        mWorkerPool = new ArrayBlockingQueue<Worker>(mWorkers.length);

        for (int i=0; i<mWorkers.length; i++) {
            Worker worker = new Worker(mSubSorters[i]);
            mWorkers[i] = worker;
            mWorkerPool.add(worker);
            worker.setDaemon(true);
            worker.start();
        }
    }

    /**
     * Add an entry into the sorter. Ownership of the key and value instances are transferred,
     * and so they must not be modified after calling this method.
     *
     * <p>If multiple entries are added with matching keys, only the last one added is kept.
     */
    public synchronized void add(byte[] key, byte[] value) throws IOException {
        Worker batch = mBatch;

        if (batch == null) {
            try {
                mBatch = batch = mWorkerPool.take();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }

        if (batch.mSorter.addx(key, value)) {
            mBatch = null;
            batch.go();
        }

        // 1. Sorter.addx
        // 2. When addx returns true, enqueue task
        // 3. Task polls for an available Sorter and adds to it
    }

    /**
     * Finish sorting the entries and return a temporary index with the results.
     */
    public Index finish() throws IOException {
        // FIXME
        return null;
    }

    /**
     * Discards all the entries and frees up space in the database.
     */
    public void reset() throws IOException {
        // FIXME
    }

    void ready(Worker worker) {
        mWorkerPool.add(worker);
    }

    class Worker extends Thread {
        final Sorter mSorter;

        private boolean mGo;

        Worker(Sorter sorter) {
            mSorter = sorter;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    synchronized (this) {
                        while (!mGo) {
                            wait();
                        }
                        mGo = false;
                    }
                    mSorter.sortx();
                    ready(this);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        synchronized void go() {
            mGo = true;
            notify();
        }
    }
}
