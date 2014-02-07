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

import java.util.concurrent.locks.Lock;

import static org.cojen.tupl.Utils.*;

/**
 * B-tree implementation.
 *
 * @author Brian S O'Neill
 */
final class Tree implements Index {
    final Database mDatabase;

    // Although tree roots can be created and deleted, the object which refers
    // to the root remains the same. Internal state is transferred to/from this
    // object when the tree root changes.
    final Node mRoot;

    final int mMaxKeySize;
    final int mMaxEntrySize;

    Tree(Database db, Node root) {
        mDatabase = db;
        mRoot = root;

        int pageSize = db.pageSize();

        // Key size is limited to ensure that internal nodes can hold at least two keys.
        // Absolute maximum is dictated by key encoding, as described in Node class.
        mMaxKeySize = Math.min(16383, (pageSize >> 1) - 22);

        // Limit maximum non-fragmented entry size to 0.75 of usable node size.
        mMaxEntrySize = ((pageSize - Node.TN_HEADER_SIZE) * 3) >> 2;
    }

    @Override
    public Cursor newCursor() {
        return new TreeCursor(this);
    }

    /**
     * @see Database#markDirty
     */
    boolean markDirty(Node node) throws IOException {
        return mDatabase.markDirty(this, node);
    }
}
