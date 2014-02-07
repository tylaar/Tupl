/*
 *  Copyright 2011-2013 Brian S O'Neill
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

import java.util.concurrent.locks.Lock;

import static org.cojen.tupl.Node.*;
import static org.cojen.tupl.Utils.*;

/**
 * Main database class, containing a collection of transactional indexes. Call
 * {@link #open open} to obtain a Database instance. Examples:
 *
 * <p>Open a non-durable database, limited to a max size of 100MB:
 *
 * <pre>
 * DatabaseConfig config = new DatabaseConfig().maxCacheSize(100_000_000);
 * Database db = Database.open(config);
 * </pre>
 *
 * @author Brian S O'Neill
 * @see DatabaseConfig
 */
public final class Database {
    private static final int DEFAULT_CACHED_NODES = 1000;
    // +2 for registry and key map root nodes, +1 for one user index, and +2
    // for usage list to function correctly. It always assumes that the least
    // recently used node points to a valid, more recently used node.
    private static final int MIN_CACHED_NODES = 5;

    // Approximate byte overhead per node. Influenced by many factors,
    // including pointer size and child node references. This estimate assumes
    // 32-bit pointers.
    private static final int NODE_OVERHEAD = 100;

    private static int nodeCountFromBytes(long bytes, int pageSize) {
        if (bytes <= 0) {
            return 0;
        }
        pageSize += NODE_OVERHEAD;
        bytes += pageSize - 1;
        if (bytes <= 0) {
            // Overflow.
            return Integer.MAX_VALUE;
        }
        long count = bytes / pageSize;
        return count <= Integer.MAX_VALUE ? (int) count : Integer.MAX_VALUE;
    }

    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static final int MINIMUM_PAGE_SIZE = 512;
    private static final int MAXIMUM_PAGE_SIZE = 65536;

    final PageDb mPageDb;
    final int mPageSize;

    private final Latch mUsageLatch;
    private int mMaxNodeCount;
    private int mNodeCount;
    private Node mMostRecentlyUsed;
    private Node mLeastRecentlyUsed;

    private final Lock mSharedCommitLock;

    // Is either CACHED_DIRTY_0 or CACHED_DIRTY_1. Access is guarded by commit lock.
    private byte mCommitState;

    private final Tree mRoot;

    private final PageAllocator mAllocator;

    /**
     * Open a database, creating it if necessary.
     */
    public static Database open(DatabaseConfig config) throws IOException {
        Database db = new Database(config);
        return db;
    }

    /**
     * @param config unshared config
     */
    private Database(DatabaseConfig config) throws IOException {
        int pageSize = config.mPageSize;
        boolean explicitPageSize = true;
        if (pageSize <= 0) {
            config.pageSize(pageSize = DEFAULT_PAGE_SIZE);
            explicitPageSize = false;
        } else if (pageSize < MINIMUM_PAGE_SIZE) {
            throw new IllegalArgumentException
                ("Page size is too small: " + pageSize + " < " + MINIMUM_PAGE_SIZE);
        } else if (pageSize > MAXIMUM_PAGE_SIZE) {
            throw new IllegalArgumentException
                ("Page size is too large: " + pageSize + " > " + MAXIMUM_PAGE_SIZE);
        } else if ((pageSize & 1) != 0) {
            throw new IllegalArgumentException
                ("Page size must be even: " + pageSize);
        }

        int minCache, maxCache;
        cacheSize: {
            long minCachedBytes = Math.max(0, config.mMinCachedBytes);
            long maxCachedBytes = Math.max(0, config.mMaxCachedBytes);

            if (maxCachedBytes == 0) {
                maxCachedBytes = minCachedBytes;
                if (maxCachedBytes == 0) {
                    minCache = maxCache = DEFAULT_CACHED_NODES;
                    break cacheSize;
                }
            }

            if (minCachedBytes > maxCachedBytes) {
                throw new IllegalArgumentException
                    ("Minimum cache size exceeds maximum: " +
                     minCachedBytes + " > " + maxCachedBytes);
            }

            minCache = nodeCountFromBytes(minCachedBytes, pageSize);
            maxCache = nodeCountFromBytes(maxCachedBytes, pageSize);

            minCache = Math.max(MIN_CACHED_NODES, minCache);
            maxCache = Math.max(MIN_CACHED_NODES, maxCache);
        }

        mUsageLatch = new Latch();
        mMaxNodeCount = maxCache;

        mPageSize = pageSize;
        mPageDb = new NonPageDb(pageSize);
        mSharedCommitLock = mPageDb.sharedCommitLock();

        // Pre-allocate nodes. They are automatically added to the usage list, and so nothing
        // special needs to be done to allow them to get used. Since the initial state is
        // clean, evicting these nodes does nothing.

        for (int i=minCache; --i>=0; ) {
            allocLatchedNode(true).releaseExclusive();
        }

        mSharedCommitLock.lock();
        try {
            mCommitState = CACHED_DIRTY_0;
        } finally {
            mSharedCommitLock.unlock();
        }

        Node rootNode = allocLatchedNode(false);
        rootNode.asEmptyRoot();
        rootNode.releaseExclusive();

        mRoot = new Tree(this, rootNode);

        mAllocator = new PageAllocator(mPageDb);
    }

    public Index rootIndex() throws IOException {
        return mRoot;
    }

    /**
     * Returns the fixed size of all pages in the store, in bytes.
     */
    int pageSize() {
        return mPageSize;
    }

    /**
     * Access the shared commit lock, which prevents commits while held.
     */
    Lock sharedCommitLock() {
        return mSharedCommitLock;
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, with an id
     * of zero and a clean state.
     */
    Node allocLatchedNode() throws IOException {
        return allocLatchedNode(true);
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, with an id
     * of zero and a clean state.
     *
     * @param evictable true if allocated node can be automatically evicted
     */
    Node allocLatchedNode(boolean evictable) throws IOException {
        final Latch usageLatch = mUsageLatch;
        usageLatch.acquireExclusive();
        alloc: {
            int max = mMaxNodeCount;

            if (max == 0) {
                break alloc;
            }

            if (mNodeCount < max) {
                try {
                    Node node = new Node(mPageSize);
                    node.acquireExclusive();
                    mNodeCount++;
                    if (evictable) {
                        if ((node.mLessUsed = mMostRecentlyUsed) == null) {
                            mLeastRecentlyUsed = node;
                        } else {
                            mMostRecentlyUsed.mMoreUsed = node;
                        }
                        mMostRecentlyUsed = node;
                    }
                    // Return with node latch still held.
                    return node;
                } finally {
                    usageLatch.releaseExclusive();
                }
            }

            if (!evictable && mLeastRecentlyUsed.mMoreUsed == mMostRecentlyUsed) {
                // Cannot allow list to shrink to less than two elements.
                break alloc;
            }

            do {
                Node node = mLeastRecentlyUsed;
                (mLeastRecentlyUsed = node.mMoreUsed).mLessUsed = null;
                node.mMoreUsed = null;
                (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
                mMostRecentlyUsed = node;

                if (!node.tryAcquireExclusive()) {
                    continue;
                }

                usageLatch.releaseExclusive();

                if ((node = Node.evict(node, mPageDb)) != null) {
                    if (!evictable) {
                        makeUnevictable(node);
                    }
                    // Return with node latch still held.
                    return node;
                }

                usageLatch.acquireExclusive();

                if (mMaxNodeCount == 0) {
                    break alloc;
                }
            } while (--max > 0);
        }

        usageLatch.releaseExclusive();
        throw new CacheExhaustedException();
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively and marked
     * dirty. Caller must hold commit lock.
     */
    Node allocDirtyNode() throws IOException {
        Node node = allocLatchedNode(true);
        try {
            dirty(node, mAllocator.allocPage(node));
            return node;
        } catch (IOException e) {
            node.releaseExclusive();
            throw e;
        }
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, marked
     * dirty and unevictable. Caller must hold commit lock.
     */
    Node allocUnevictableNode() throws IOException {
        Node node = allocLatchedNode(false);
        try {
            dirty(node, mAllocator.allocPage(node));
            return node;
        } catch (IOException e) {
            makeEvictableNow(node);
            node.releaseExclusive();
            throw e;
        }
    }

    /**
     * Allow a Node which was allocated as unevictable to be evictable,
     * starting off as the most recently used.
     */
    void makeEvictable(Node node) {
        final Latch usageLatch = mUsageLatch;
        usageLatch.acquireExclusive();
        try {
            if (mMaxNodeCount == 0) {
                // Closed.
                return;
            }
            if (node.mMoreUsed != null || node.mLessUsed != null) {
                throw new IllegalStateException();
            }
            (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
            mMostRecentlyUsed = node;
        } finally {
            usageLatch.releaseExclusive();
        }
    }

    /**
     * Allow a Node which was allocated as unevictable to be evictable, as the
     * least recently used.
     */
    void makeEvictableNow(Node node) {
        final Latch usageLatch = mUsageLatch;
        usageLatch.acquireExclusive();
        try {
            if (mMaxNodeCount == 0) {
                // Closed.
                return;
            }
            if (node.mMoreUsed != null || node.mLessUsed != null) {
                throw new IllegalStateException();
            }
            (node.mMoreUsed = mLeastRecentlyUsed).mLessUsed = node;
            mLeastRecentlyUsed = node;
        } finally {
            usageLatch.releaseExclusive();
        }
    }

    /**
     * Allow a Node which was allocated as evictable to be unevictable.
     */
    void makeUnevictable(final Node node) {
        final Latch usageLatch = mUsageLatch;
        usageLatch.acquireExclusive();
        try {
            if (mMaxNodeCount == 0) {
                // Closed.
                return;
            }
            final Node lessUsed = node.mLessUsed;
            final Node moreUsed = node.mMoreUsed;
            if (lessUsed == null) {
                (mLeastRecentlyUsed = moreUsed).mLessUsed = null;
            } else if (moreUsed == null) {
                (mMostRecentlyUsed = lessUsed).mMoreUsed = null;
            } else {
                lessUsed.mMoreUsed = moreUsed;
                moreUsed.mLessUsed = lessUsed;
            }
            node.mMoreUsed = null;
            node.mLessUsed = null;
        } finally {
            usageLatch.releaseExclusive();
        }
    }

    /**
     * Caller must hold commit lock and any latch on node.
     */
    boolean shouldMarkDirty(Node node) {
        return node.mCachedState != mCommitState && node.mId != Node.STUB_ID;
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Method does
     * nothing if node is already dirty. Latch is never released by this method,
     * even if an exception is thrown.
     *
     * @return true if just made dirty and id changed
     */
    boolean markDirty(Tree tree, Node node) throws IOException {
        if (node.mCachedState == mCommitState || node.mId == Node.STUB_ID) {
            return false;
        } else {
            doMarkDirty(tree, node);
            return true;
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Method must
     * not be called if node is already dirty. Latch is never released by this
     * method, even if an exception is thrown.
     */
    void doMarkDirty(Tree tree, Node node) throws IOException {
    }

    /**
     * Caller must hold commit lock and exclusive latch on node.
     */
    private void dirty(Node node, long newId) {
        node.mId = newId;
        node.mCachedState = mCommitState;
    }

    /**
     * Indicate that a non-root node is most recently used. Root node is not
     * managed in usage list and cannot be evicted. Caller must hold any latch
     * on node. Latch is never released by this method, even if an exception is
     * thrown.
     */
    void used(Node node) {
        // Because this method can be a bottleneck, don't wait for exclusive
        // latch. If node is popular, it will get more chances to be identified
        // as most recently used. This strategy works well enough because cache
        // eviction is always a best-guess approach.
        final Latch usageLatch = mUsageLatch;
        if (usageLatch.tryAcquireExclusive()) {
            Node moreUsed = node.mMoreUsed;
            if (moreUsed != null) {
                Node lessUsed = node.mLessUsed;
                if ((moreUsed.mLessUsed = lessUsed) == null) {
                    mLeastRecentlyUsed = moreUsed;
                } else {
                    lessUsed.mMoreUsed = moreUsed;
                }
                node.mMoreUsed = null;
                (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
                mMostRecentlyUsed = node;
            }
            usageLatch.releaseExclusive();
        }
    }

    byte[] removeSpareBuffer() {
        return new byte[mPageSize];
    }

    void addSpareBuffer(byte[] buffer) {
        // FIXME: delete buffer
    }
}
