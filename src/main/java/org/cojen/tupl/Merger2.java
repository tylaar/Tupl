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
public class Merger2 implements Runnable {
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

        if (true) {
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
        }

        Tree[] sources = new Tree[ixCount];
        for (int i=0; i<ixCount; i++) {
            sources[i] = (Tree) db.openIndex("ix" + i);
        }

        Tree target = (Tree) db.openIndex("target");

        Merger2 m = new Merger2(target, sources);

        m.run();

        db.shutdown();
    }

    private final Tree mTarget;
    private final Tree[] mSources;

    private volatile Throwable mException;

    public Merger2(Tree target, Tree... sources) {
        mTarget = target;
        mSources = sources;
    }

    public void run() {
        Tree[] sources = mSources;

        PriorityQueue<Selector> pq = new PriorityQueue<>(sources.length);

        try {
            for (int i=0; i<sources.length; i++) {
                Tree source = sources[i];
                TreeCursor c = source.newCursor(Transaction.BOGUS);
                c.first();
                if (c.key() != null) {
                    pq.add(new Selector(source, i, c));
                }
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
                    TreeCursor c = sel.mCursor;
                    byte[] key = c.key();
                    if (!Arrays.equals(key, last)) {
                        target.append(key, c.value(), leaf);
                    }
                    last = key;
                    c.trim();
                    if (c.key() == null) {
                        sel.mTree.drop();
                    } else {
                        pq.add(sel);
                    }
                }
            } finally {
                TreeCursorFrame.popAll(leaf);
            }
        } catch (Throwable e) {
            mException = e;
        } finally {
            for (Selector sel : pq) {
                sel.mCursor.reset();
            }
        }
    }

    public void exceptionCheck() throws DatabaseException {
        Throwable e = mException;
        if (e != null) {
            throw new DatabaseException(e);
        }
    }

    private static class Selector implements Comparable<Selector> {
        final Tree mTree;
        final int mOrder;
        final TreeCursor mCursor;

        /**
         * @param tree must not be empty
         * @param order must be unique
         * @param cursor must be positioned
         */
        Selector(Tree tree, int order, TreeCursor cursor) {
            mTree = tree;
            mOrder = order;
            mCursor = cursor;
        }

        @Override
        public int compareTo(Selector other) {
            int compare = Utils.compareKeys(this.mCursor.key(), other.mCursor.key());
            if (compare == 0) {
                // Favor the later tree when duplicates are found.
                compare = this.mOrder < other.mOrder ? 1 : -1;
            }
            return compare;
        }
    }
}
