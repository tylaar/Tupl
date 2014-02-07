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

import static org.cojen.tupl.Utils.*;

/**
 * Internal cursor implementation, which can be used by one thread at a time.
 *
 * @author Brian S O'Neill
 */
final class TreeCursor implements Cursor {
    final Tree mTree;

    // Top stack frame for cursor, always a leaf.
    private TreeCursorFrame mLeaf;

    byte[] mKey;
    byte[] mValue;

    TreeCursor(Tree tree) {
        mTree = tree;
    }

    @Override
    public byte[] key() {
        return mKey;
    }

    @Override
    public byte[] value() {
        return mValue;
    }

    @Override
    public void find(byte[] key) throws IOException {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }

        mKey = key;
        Node node = mTree.mRoot;
        TreeCursorFrame frame = reset(node);

        while (true) {
            if (node.isLeaf()) {
                int pos;
                if (node.mSplit == null) {
                    pos = node.binarySearch(key);
                    frame.bind(node, pos);
                } else {
                    pos = node.mSplit.binarySearch(node, key);
                    frame.bind(node, pos);
                    if (pos < 0) {
                        // The finishSplit method will release the latch, and
                        // so the frame must be completely defined first.
                        frame.mNotFoundKey = key;
                    }
                    node = finishSplit(frame, node);
                    pos = frame.mNodePos;
                }

                mLeaf = frame;

                if (pos < 0) {
                    frame.mNotFoundKey = key;
                    mValue = null;
                    node.releaseExclusive();
                } else {
                    try {
                        mValue = node.retrieveLeafValue(mTree, pos);
                    } catch (Throwable e) {
                        mValue = NOT_LOADED;
                        node.releaseExclusive();
                        throw e;
                    }
                    node.releaseExclusive();
                }
                return;
            }

            Split split = node.mSplit;
            if (split == null) {
                int childPos = Node.internalPos(node.binarySearch(key));
                frame.bind(node, childPos);
                try {
                    node = latchChild(node, childPos, true);
                } catch (Throwable e) {
                    cleanup(frame);
                    throw e;
                }
            } else {
                // Follow search into split, binding this frame to the unsplit
                // node as if it had not split. The binding will be corrected
                // when split is finished.

                final Node sibling = split.latchSibling();

                final Node left, right;
                if (split.mSplitRight) {
                    left = node;
                    right = sibling;
                } else {
                    left = sibling;
                    right = node;
                }

                final Node selected;
                final int selectedPos;

                if (split.compare(key) < 0) {
                    selected = left;
                    selectedPos = Node.internalPos(left.binarySearch(key));
                    frame.bind(node, selectedPos);
                    right.releaseExclusive();
                } else {
                    selected = right;
                    selectedPos = Node.internalPos(right.binarySearch(key));
                    frame.bind(node, left.highestInternalPos() + 2 + selectedPos);
                    left.releaseExclusive();
                }

                try {
                    node = latchChild(selected, selectedPos, true);
                } catch (Throwable e) {
                    cleanup(frame);
                    throw e;
                }
            }

            frame = new TreeCursorFrame(frame);
        }
    }

    /**
     * Must be called with node latch not held.
     */
    private void doLoad() throws IOException {
        byte[] key = mKey;
        if (key == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }

        TreeCursorFrame frame = leafSharedNotSplit();
        Node node = frame.mNode;
        try {
            int pos = frame.mNodePos;
            mValue = pos >= 0 ? node.retrieveLeafValue(mTree, pos) : null;
        } finally {
            node.releaseShared();
        }
    }

    @Override
    public void store(byte[] value) throws IOException {
        byte[] key = mKey;
        if (key == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }
        try {
            store(leafExclusive(), value);
        } catch (Throwable e) {
            throw e;
        }
    }

    /**
     * Shared commit lock is acquired, to prevent checkpoints from observing in-progress
     * splits.
     *
     * @param leaf leaf frame, latched exclusively, which is always released by this method
     */
    private void store(final TreeCursorFrame leaf, final byte[] value)
        throws IOException
    {
        byte[] key = mKey;
        Node node;

        final Lock sharedCommitLock = mTree.mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            // Update and insert always dirty the node. Releases latch if an exception is
            // thrown.
            node = notSplitDirty(leaf);
            final int pos = leaf.mNodePos;

            if (pos >= 0) {
                // Update entry...
                // FIXME
            } else {
                // Insert entry...

                try {
                    int encodedKeyLen = Node.calculateKeyLengthChecked(mTree, key);
                    node.insertLeafEntry(mTree, ~pos, key, encodedKeyLen, value);
                } catch (Throwable e) {
                    node.releaseExclusive();
                    throw e;
                }

                // Releases latch if an exception is thrown.
                node = postInsert(leaf, node, key);
            }
        } finally {
            sharedCommitLock.unlock();
        }

        if (node != null) {
            node.releaseExclusive();
            mValue = value;
        }
    }

    /**
     * Fixes this and all bound cursors after an insert.
     *
     * @param leaf latched leaf frame; released if an exception is thrown
     * @return replacement node
     */
    private Node postInsert(TreeCursorFrame leaf, Node node, byte[] key) throws IOException {
        int pos = leaf.mNodePos;
        int newPos = ~pos;

        leaf.mNodePos = newPos;
        leaf.mNotFoundKey = null;

        // Fix all cursors bound to the node.
        TreeCursorFrame frame = node.mLastCursorFrame;
        do {
            if (frame == leaf) {
                // Don't need to fix self.
                continue;
            }

            int framePos = frame.mNodePos;

            if (framePos == pos) {
                // Other cursor is at same not-found position as this one was. If keys are the
                // same, then other cursor switches to a found state as well. If key is
                // greater, then position needs to be updated.

                byte[] frameKey = frame.mNotFoundKey;
                int compare = compareKeys(frameKey, 0, frameKey.length, key, 0, key.length);
                if (compare > 0) {
                    // Position is a complement, so subtract instead of add.
                    frame.mNodePos = framePos - 2;
                } else if (compare == 0) {
                    frame.mNodePos = newPos;
                    frame.mNotFoundKey = null;
                }
            } else if (framePos >= newPos) {
                frame.mNodePos = framePos + 2;
            } else if (framePos < pos) {
                // Position is a complement, so subtract instead of add.
                frame.mNodePos = framePos - 2;
            }
        } while ((frame = frame.mPrevCousin) != null);

        if (node.mSplit != null) {
            node = finishSplit(leaf, node);
        }

        return node;
    }

    @Override
    public void reset() {
        TreeCursorFrame frame = mLeaf;
        mLeaf = null;
        mKey = null;
        mValue = null;
        if (frame != null) {
            TreeCursorFrame.popAll(frame);
        }
    }

    /**
     * Called if an exception is thrown while frames are being constructed.
     * Given frame does not need to be bound, but it must not be latched.
     */
    private void cleanup(TreeCursorFrame frame) {
        mLeaf = frame;
        reset();
    }

    /**
     * Resets all frames and latches root node, exclusively. Although the
     * normal reset could be called directly, this variant avoids unlatching
     * the root node, since a find operation would immediately relatch it.
     *
     * @return new or recycled frame
     */
    private TreeCursorFrame reset(Node root) {
        TreeCursorFrame frame = mLeaf;
        if (frame == null) {
            // Allocate new frame before latching root -- allocation can block.
            frame = new TreeCursorFrame();
            root.acquireExclusive();
            return frame;
        }

        mLeaf = null;

        while (true) {
            Node node = frame.acquireExclusive();
            TreeCursorFrame parent = frame.pop();

            if (parent == null) {
                // Usually the root frame refers to the root node, but it
                // can be wrong if the tree height is changing.
                if (node != root) {
                    node.releaseExclusive();
                    root.acquireExclusive();
                }
                return frame;
            }

            node.releaseExclusive();
            frame = parent;
        }
    }

    /**
     * Checks that leaf is defined and returns it.
     */
    private TreeCursorFrame leaf() {
        TreeCursorFrame leaf = mLeaf;
        if (leaf == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }
        return leaf;
    }

    /**
     * Latches and returns leaf frame, which might be split.
     */
    private TreeCursorFrame leafExclusive() {
        TreeCursorFrame leaf = leaf();
        leaf.acquireExclusive();
        return leaf;
    }

    /**
     * Latches and returns leaf frame, not split.
     *
     * @throws IllegalStateException if unpositioned
     */
    TreeCursorFrame leafExclusiveNotSplit() throws IOException {
        TreeCursorFrame leaf = leaf();
        Node node = leaf.acquireExclusive();
        if (node.mSplit != null) {
            finishSplit(leaf, node);
        }
        return leaf;
    }

    /**
     * Latches and returns leaf frame, not split. Caller must hold shared commit lock.
     *
     * @throws IllegalStateException if unpositioned
     */
    TreeCursorFrame leafExclusiveNotSplitDirty() throws IOException {
        TreeCursorFrame frame = leafExclusive();
        notSplitDirty(frame);
        return frame;
    }

    /**
     * Latches and returns leaf frame, not split.
     *
     * @throws IllegalStateException if unpositioned
     */
    TreeCursorFrame leafSharedNotSplit() throws IOException {
        TreeCursorFrame leaf = leaf();
        Node node = leaf.acquireShared();
        if (node.mSplit != null) {
            doSplit: {
                if (!node.tryUpgrade()) {
                    node.releaseShared();
                    node = leaf.acquireExclusive();
                    if (node.mSplit == null) {
                        break doSplit;
                    }
                }
                node = finishSplit(leaf, node);
            }
            node.downgrade();
        }
        return leaf;
    }

    /**
     * Called with exclusive frame latch held, which is retained. Leaf frame is
     * dirtied, any split is finished, and the same applies to all parent
     * nodes. Caller must hold shared commit lock, to prevent deadlock. Node
     * latch is released if an exception is thrown.
     *
     * @return replacement node, still latched
     */
    private Node notSplitDirty(final TreeCursorFrame frame) throws IOException {
        Node node = frame.mNode;

        if (node.mSplit != null) {
            // Already dirty, but finish the split.
            return finishSplit(frame, node);
        }

        Database db = mTree.mDatabase;
        if (!db.shouldMarkDirty(node)) {
            return node;
        }

        TreeCursorFrame parentFrame = frame.mParentFrame;
        if (parentFrame == null) {
            try {
                db.doMarkDirty(mTree, node);
                return node;
            } catch (Throwable e) {
                node.releaseExclusive();
                throw e;
            }
        }

        // Make sure the parent is not split and dirty too.
        Node parentNode;
        doParent: {
            parentNode = parentFrame.tryAcquireExclusive();
            if (parentNode == null) {
                node.releaseExclusive();
                parentFrame.acquireExclusive();
            } else if (parentNode.mSplit != null || db.shouldMarkDirty(parentNode)) {
                node.releaseExclusive();
            } else {
                break doParent;
            }
            parentNode = notSplitDirty(parentFrame);
            node = frame.acquireExclusive();
        }

        while (node.mSplit != null) {
            // Already dirty now, but finish the split. Since parent latch is
            // already held, no need to call into the regular finishSplit
            // method. It would release latches and recheck everything.
            parentNode.insertSplitChildRef(mTree, parentFrame.mNodePos, node);
            if (parentNode.mSplit != null) {
                parentNode = finishSplit(parentFrame, parentNode);
            }
            node = frame.acquireExclusive();
        }
        
        try {
            if (db.markDirty(mTree, node)) {
                parentNode.updateChildRefId(parentFrame.mNodePos, node.mId);
            }
            return node;
        } catch (Throwable e) {
            node.releaseExclusive();
            throw e;
        } finally {
            parentNode.releaseExclusive();
        }
    }

    /**
     * Caller must hold exclusive latch and it must verify that node has
     * split. Node latch is released if an exception is thrown.
     *
     * @return replacement node, still latched
     */
    private Node finishSplit(final TreeCursorFrame frame, Node node) throws IOException {
        Tree tree = mTree;

        while (node == tree.mRoot) {
            try {
                node.finishSplitRoot(tree);
                // Must return the node as referenced by the frame, which is no
                // longer the root node.
                node.releaseExclusive();
                return frame.acquireExclusive();
            } catch (Throwable e) {
                node.releaseExclusive();
                throw e;
            }
        }

        final TreeCursorFrame parentFrame = frame.mParentFrame;
        node.releaseExclusive();

        Node parentNode = parentFrame.acquireExclusive();
        while (true) {
            if (parentNode.mSplit != null) {
                parentNode = finishSplit(parentFrame, parentNode);
            }
            node = frame.acquireExclusive();
            if (node.mSplit == null) {
                parentNode.releaseExclusive();
                return node;
            }
            parentNode.insertSplitChildRef(tree, parentFrame.mNodePos, node);
        }
    }

    /**
     * With parent held exclusively, returns child with exclusive latch held.
     * If an exception is thrown, parent and child latches are always released.
     *
     * @return child node, possibly split
     */
    private Node latchChild(Node parent, int childPos, boolean releaseParent)
        throws IOException
    {
        Node childNode = parent.mChildNodes[childPos >> 1];
        long childId = parent.retrieveChildRefId(childPos);

        if (childNode != null && childId == childNode.mId) {
            childNode.acquireExclusive();
            // Need to check again in case evict snuck in.
            if (childId != childNode.mId) {
                childNode.releaseExclusive();
            } else {
                if (releaseParent) {
                    parent.releaseExclusive();
                }
                mTree.mDatabase.used(childNode);
                return childNode;
            }
        }

        throw new DatabaseException("Non durable database");
    }
}
