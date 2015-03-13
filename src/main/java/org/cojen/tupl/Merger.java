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
import java.util.PriorityQueue;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class Merger {
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

        final int ixCount = 10;

        if (false) {
            byte[] key = new byte[6];
            byte[] value = new byte[0];
            java.util.Random rnd = new java.util.Random(8926489);

            final int count = 50_000_000;
            for (int i=0; i<count; i++) {
                if (i % 100_000 == 0) {
                    System.out.println(i);
                }
                Index ix = db.openIndex("ix" + (i % ixCount));
                rnd.nextBytes(key);
                ix.store(Transaction.BOGUS, key, value);
                key = key.clone();
            }

            db.shutdown();
            return;
        }

        Tree[] sources = new Tree[ixCount];
        for (int i=0; i<ixCount; i++) {
            sources[i] = (Tree) db.openIndex("ix" + i);
        }

        Merger m = new Merger(null, 10, sources);

        m.merge();
    }

    private final Tree mTarget;
    private final WorkerPool mWorkerPool;
    private final Selector[] mSelectors;

    public Merger(Tree target, int maxThreads, Tree... sources) throws IOException {
        mTarget = target;

        mWorkerPool = new WorkerPool(Math.min(maxThreads, sources.length));

        mSelectors = new Selector[sources.length];
        for (int i=0; i<sources.length; i++) {
            Tree source = sources[i];
            TreeCursor cursor = source.newCursor(Transaction.BOGUS);

            Fetcher fetcher = new Fetcher(mWorkerPool, cursor, 100) { // FIXME: config capacity
                @Override
                protected void advance(TreeCursor c) throws IOException {
                    c.trim();
                }
            };

            mSelectors[i] = new Selector(source, i, fetcher);
        }

        mWorkerPool.init();

        try {
            for (Selector sel : mSelectors) {
                sel.mFetcher.init();
            }
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public void merge() throws IOException {
        try {
            PriorityQueue<Selector> pq = new PriorityQueue<>(mSelectors.length);

            for (Selector sel : mSelectors) {
                pq.add(sel);
            }

            Tree target = mTarget;

            TreeCursorFrame leaf = new TreeCursorFrame();
            leaf.bind(target.mRoot, 0);
            try {
                byte[] last = null;

                while (true) {
                    Selector sel = pq.poll();
                    if (sel == null) {
                        break;
                    }

                    byte[] key = sel.key();
                    byte[] value = sel.consumeValue();

                    if (key == null) {
                        System.out.println("no key: " + sel.mSource);
                        continue;
                    }

                    if (!Arrays.equals(key, last)) {
                        target.append(key, value, leaf);
                    }

                    last = key;
                    pq.add(sel);
                }
            } finally {
                TreeCursorFrame.popAll(leaf);
            }
        } finally {
            close();
        }

        // FIXME: check for exceptions
    }

    public void close() {
        mWorkerPool.close();
        for (Selector sel : mSelectors) {
            sel.mFetcher.close();
        }
    }

    final class Selector implements Comparable<Selector> {
        final View mSource;
        private final int mOrder;
        private final Fetcher mFetcher;

        private final byte[][] mEntries[];

        private byte[][] mKeys;
        private byte[][] mValues;

        private int mFirstPos; // inclusive
        private int mLastPos;  // exclusive

        Selector(View source, int order, Fetcher fetcher) {
            mSource = source;
            mOrder = order;
            mFetcher = fetcher;
            mEntries = new byte[2][][];
        }

        @Override
        public final int compareTo(Selector other) {
            byte[] thisKey = this.key();
            byte[] otherKey = other.key();

            int compare;
            if (thisKey == null) {
                // Allow finished cursor to percolate up.
                compare = -1;
            } else if (otherKey == null) {
                // Allow finished cursor to percolate up.
                compare = 1;
            } else {
                compare = Utils.compareKeys(thisKey, otherKey);
                if (compare == 0) {
                    // Favor the later view when duplicates are found.
                    compare = this.mOrder < other.mOrder ? 1 : -1;
                }
            }

            return compare;
        }

        public byte[] key() {
            int pos = mFirstPos;

            if (pos >= mLastPos) {
                byte[][] entries[] = mEntries;
                int count = mFetcher.nextBatch(entries);
                mKeys = entries[0];
                mValues = entries[1];
                mFirstPos = 0;
                mLastPos = count;
                pos = 0;
            }

            return mKeys[pos];
        }

        public byte[] consumeValue() {
            int pos = mFirstPos;
            byte[] value = mValues[pos];
            mFirstPos = pos + 1;
            return value;
        }
    }
}
