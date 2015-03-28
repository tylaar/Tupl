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

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Testing.
 *
 * @author Brian S O'Neill
 */
public class Sorter2 {
    /**
     * Testing.
     */
    public static void main(String[] args) throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .checkpointSizeThreshold(0)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            //.checkpointRate(-1, null)
            .minCacheSize(100_000_000L)
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

        if (false) {
            Sorter2 s = new Sorter2(db, 10_000_000, 1000);

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

        if (true) {
            Index ix;
            findIndex: {
                View all = db.indexRegistryByName();
                Cursor c = all.newCursor(null);
                c.autoload(false);
                for (c.first(); c.key() != null; c.next()) {
                    String name = new String(c.key());
                    if (name.startsWith("temp-")) {
                        ix = db.findIndex(name);
                        break findIndex;
                    }
                }
                throw new Exception("Index not found");
            }

            Cursor c = ix.newCursor(null);
            int count = 0;
            for (c.first(); c.key() != null; c.next()) {
                if (count % 100000 == 0) {
                    System.out.println(Utils.toHex(c.key()));
                }
                count++;
            }
            System.out.println("count: " + count);
        }

        db.checkpoint();
    }

    private final Database mDatabase;
    private final int mMaxMemory;
    private final int mMaxChunks;

    private Node[] mNodes;
    private int mNodeCount;
    private int mTotalSize;

    private final List<Tree> mTrees;

    private final String mPrefix;

    /**
     * @param maxMemory approximate maximum amount of memory to use for sorting; does not
     * influence database cache usage
     * @param maxChunks maximum amount of sorted chunks to maintain before merging together
     */
    public Sorter2(Database db, int maxMemory, int maxChunks) {
        this(db, maxMemory, maxChunks, "");
    }

    Sorter2(Database db, int maxMemory, int maxChunks, String prefix) {
        if (db == null) {
            throw new IllegalArgumentException();
        }

        mDatabase = db;
        mMaxMemory = maxMemory;
        mMaxChunks = Math.max(2, maxChunks);

        mTrees = new ArrayList<>();

        mPrefix = prefix;
    }

    /**
     * Add an entry into the sorter. Ownership of the key and value instances are transferred,
     * and so they must not be modified after calling this method.
     *
     * <p>If multiple entries are added with matching keys, only the last one added is kept.
     */
    public synchronized void add(byte[] key, byte[] value) throws IOException {
        Node[] nodes = mNodes;

        if (nodes == null) {
            // Start small and double as needed.
            mNodes = nodes = new Node[100];
        }

        int count = mNodeCount;

        Node node;
        obtainNode: {
            if (count < nodes.length) {
                node = nodes[count];
                if (node != null) {
                    break obtainNode;
                }
            } else {
                Node[] newNodes = new Node[nodes.length << 1];
                System.arraycopy(nodes, 0, newNodes, 0, nodes.length);
                mNodes = nodes = newNodes;
            }
            node = new Node();
            nodes[count] = node;
        }

        node.mKey = key;
        node.mValue = value;
        mNodeCount = ++count;

        // Compute total size, including estimated overhead.
        int total = mTotalSize + key.length + value.length + (12 * 2);

        if (total < mMaxMemory) {
            mTotalSize = total;
            return;
        }

        Tree tree = sortAndFillTree();

        if (tree != null) {
            mTrees.add(tree);
        }

        mNodeCount = 0;
        mTotalSize = 0;

        // Prepare nodes for re-use.
        for (int i=0; i<count; i++) {
            node = nodes[i];
            node.mKey = null;
            node.mValue = null;
        }

        if (mTrees.size() >= mMaxChunks) {
            mTrees.add(merge());
        }
    }

    /**
     * Finish sorting the entries and return a temporary index with the results.
     */
    public synchronized Index finish() throws IOException {
        sortAndFillTree();

        // Discard some objects as soon as possible.
        mNodes = null;
        mNodeCount = 0;
        mTotalSize = 0;

        if (mTrees.size() == 1) {
            Tree tree = mTrees.get(0);
            mTrees.clear();
            return tree;
        }

        return merge();
    }

    /**
     * Merges all the trees into a single tree.
     */
    private Tree merge() throws IOException {
        Tree[] trees;
        Tree target;

        synchronized (this) {
            target = createTempTree();
            trees = mTrees.toArray(new Tree[mTrees.size()]);
            mTrees.clear();
        }

        // FIXME: shared WorkerPool
        Merger merger = new Merger(target, 32, trees);
        try {
            merger.merge();
        } finally {
            merger.close();
        }

        for (Tree tree : trees) {
            tree.drop();
        }

        return target;
    }

    /**
     * Discards all the entries and frees up space in the database.
     */
    public synchronized void reset() throws IOException {
        mNodes = null;
        mNodeCount = 0;
        mTotalSize = 0;

        for (Tree tree : mTrees) {
            mDatabase.deleteIndex(tree);
        }

        mTrees.clear();
    }

    private Tree sortAndFillTree() throws IOException {
        if (mNodeCount <= 0) {
            return null;
        }

        // Must be a stable sort, which it is.
        Arrays.sort(mNodes, 0, mNodeCount);
        //Arrays.parallelSort(mNodes, 0, mNodeCount);

        try {
            Tree tree = createTempTree();

            TreeCursorFrame leaf = new TreeCursorFrame();
            leaf.bind(tree.mRoot, 0);
            try {
                Node[] nodes = mNodes;
                byte[] last = null;
                // Results are descending, so iterate in reverse to be ascending. Duplicates
                // appear such that the last one added is seen first.
                for (int i=mNodeCount; --i>=0; ) {
                    Node node = nodes[i];
                    byte[] key = node.mKey;
                    if (!Arrays.equals(key, last)) {
                        tree.append(key, node.mValue, leaf);
                    }
                    last = key;
                }
            } finally {
                TreeCursorFrame.popAll(leaf);
            }

            return tree;
        } catch (Throwable e) {
            throw cleanup(e);
        }
    }

    private int mTempCount;

    private synchronized Tree createTempTree() throws IOException {
        // FIXME: Support temp indexes. When renamed, they're not temp anymore.
        return (Tree) mDatabase.openIndex(mPrefix + "temp-" + mTempCount++);
    }

    private RuntimeException cleanup(Throwable e) {
        e.printStackTrace(System.out);
        try {
            reset();
        } catch (Throwable e2) {
            // Ignore.
        }
        throw Utils.rethrow(e);
    }

    private static class Node implements Comparable<Node> {
        private byte[] mKey;
        private byte[] mValue;

        @Override
        public int compareTo(Node other) {
            // Sort in descending order, to simplify duplicate detection.
            return Utils.compareKeys(other.mKey, this.mKey);
        }
    }
}