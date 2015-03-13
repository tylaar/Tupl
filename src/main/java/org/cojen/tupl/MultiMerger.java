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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import java.io.IOException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class MultiMerger {
    private final Database mDatabase;
    private final int mLevelMax;

    private final List<List<Tree>> mLevels;

    private int mActiveMerges;

    private Throwable mException;

    // FIXME: use proper temporary indexes
    private int mTempId;

    MultiMerger(Database db, int levelMax) {
        mDatabase = db;
        mLevelMax = Math.max(2, levelMax);
        mLevels = new ArrayList<>();
    }

    public void add(Tree source) throws IOException {
        add(source, 0, false);
    }

    public synchronized Tree finish() throws IOException {
        while (true) {
            exceptionCheck();
            if (mActiveMerges <= 0) {
                break;
            }
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }

        if (mLevels == null) {
            return null;
        }

        List<Tree> all = new ArrayList<>();

        for (List<Tree> sources : mLevels) {
            if (sources != null) {
                for (Tree source : sources) {
                    all.add(source);
                }
            }
        }

        mLevels.clear();

        if (all.isEmpty()) {
            return null;
        }

        if (all.size() == 1) {
            return all.get(0);
        }

        mActiveMerges++;
        System.out.println("active merges: " + mActiveMerges);

        try {
            Tree target = (Tree) mDatabase.openIndex("temp-0-" + (mTempId++));
            new Merger(target, -1, all).run();
            return target;
        } catch (Throwable e) {
            mergeFinished();
            throw e;
        }
    }

    void add(Tree source, int levelIx, boolean fromMerger) throws IOException {
        List<Tree> level;
        int targetId;

        synchronized (this) {
            exceptionCheck();

            List<List<Tree>> levels = mLevels;
            while (levelIx >= levels.size()) {
                levels.add(null);
            }

            level = levels.get(levelIx);
            if (level == null) {
                level = new ArrayList<>();
                levels.set(levelIx, level);
            }

            level.add(source);

            if (level.size() < mLevelMax) {
                return;
            }

            levels.set(levelIx, null);
            targetId = mTempId++;

            mActiveMerges++;
            System.out.println("active merges: " + mActiveMerges);
        }

        try {
            Tree target = (Tree) mDatabase.openIndex("temp-0-" + targetId);
            Merger merger = new Merger(target, levelIx + 1, level);

            if (fromMerger) {
                merger.run();
            } else {
                // FIXME: use a thread pool which blocks until any are available
                new Thread(merger).start();
            }
        } catch (Throwable e) {
            mergeFinished();
            throw e;
        }
    }

    synchronized void mergeFailed(Throwable e) {
        if (mException == null) {
            mException = e;
        }
    }

    synchronized void mergeFinished() {
        mActiveMerges--;
        System.out.println("active merges: " + mActiveMerges);
        notify();
    }

    private void exceptionCheck() throws IOException {
        Throwable e = mException;
        if (e != null) {
            throw new IOException(e);
        }
    }

    class Merger implements Runnable {
        private final Tree mTarget;
        private final int mTargetLevel;
        private final List<Tree> mSources;

        public Merger(Tree target, int targetLevel, List<Tree> sources) {
            mTarget = target;
            mTargetLevel = targetLevel;
            mSources = sources;
        }

        public void run() {
            List<Tree> sources = mSources;

            PriorityQueue<Selector> pq = new PriorityQueue<>(sources.size());

            try {
                for (int i=0; i<sources.size(); i++) {
                    Tree source = sources.get(i);
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

                if (mTargetLevel >= 0) {
                    add(target, mTargetLevel, true);
                }
            } catch (Throwable e) {
                mergeFailed(e);
            } finally {
                for (Selector sel : pq) {
                    sel.mCursor.reset();
                }
                mergeFinished();
            }
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
