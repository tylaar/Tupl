/*
 *  Copyright 2012-2013 Brian S O'Neill
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

/**
 * Tracks a list of pages which were allocated, allowing them to be iterated
 * over in the original order.
 *
 * @author Brian S O'Neill
 */
final class PageAllocator {
    private final PageDb mPageDb;
    private final Latch mLatch;

    // Linked list of dirty nodes.
    private Node mFirstDirty;
    private Node mLastDirty;

    // Iterator over dirty nodes.
    private Node mFlushNext;

    PageAllocator(PageDb source) {
        mPageDb = source;
        mLatch = new Latch();
    }

    /**
     * @param forNode node which needs a new page; must be latched
     */
    long allocPage(Node forNode) throws IOException {
        // When allocations are in order, the list maintains the order.
        dirty(forNode);
        return mPageDb.allocPage();
    }

    /**
     * Move or add node to the end of the dirty list.
     */
    void dirty(Node node) {
        final Latch latch = mLatch;
        latch.acquireExclusive();
        try {
            final Node next = node.mNextDirty;
            final Node prev = node.mPrevDirty;
            if (next != null) {
                if ((next.mPrevDirty = prev) == null) {
                    mFirstDirty = next;
                } else {
                    prev.mNextDirty = next;
                }
                node.mNextDirty = null;
                (node.mPrevDirty = mLastDirty).mNextDirty = node;
            } else if (prev == null) {
                Node last = mLastDirty;
                if (last == node) {
                    return;
                }
                if (last == null) {
                    mFirstDirty = node;
                } else {
                    node.mPrevDirty = last;
                    last.mNextDirty = node;
                }
            }
            mLastDirty = node;
            // See flushNextDirtyNode for explanation for node latch requirement.
            if (mFlushNext == node) {
                mFlushNext = next;
            }
        } finally {
            latch.releaseExclusive();
        }
    }
}
