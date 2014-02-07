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

import static java.lang.System.arraycopy;

import static org.cojen.tupl.Utils.*;

/**
 * Node within a B-tree, undo log, or a large value fragment.
 *
 * @author Brian S O'Neill
 */
final class Node extends Latch {
    // Note: Changing these values affects how the Database class handles the
    // commit flag. It only needs to flip bit 0 to switch dirty states.
    static final byte
        CACHED_CLEAN     = 0, // 0b0000
        CACHED_DIRTY_0   = 2, // 0b0010
        CACHED_DIRTY_1   = 3; // 0b0011

    /*
      Node type encoding strategy:

      bits 7..4: major type   0010 (fragment), 0100 (undo log),
                              0110 (internal), 0111 (bottom internal), 1000 (leaf)
      bits 3..1: sub type     for leaf: x0x (normal)
                              for internal: x0x (6 byte child pointers), x1x (8 byte pointers)
                              for both: bit 1 is set if low extremity, bit 3 for high extremity
      bit  0:    endianness   0 (little), 1 (big)

      TN == Tree Node

      Note that leaf type is always negative. If type encoding changes, the
      isLeaf method might need to be updated.

     */

    static final byte
        TYPE_NONE     = 0,
        TYPE_FRAGMENT = (byte) 0x20, // 0b0010_000_0
        TYPE_UNDO_LOG = (byte) 0x40, // 0b0100_000_0
        TYPE_TN_IN    = (byte) 0x64, // 0b0110_010_0
        TYPE_TN_BIN   = (byte) 0x74, // 0b0111_010_0
        TYPE_TN_LEAF  = (byte) 0x80; // 0b1000_000_0

    static final byte LOW_EXTREMITY = 0x02, HIGH_EXTREMITY = 0x08;

    // Tree node header size.
    static final int TN_HEADER_SIZE = 12;

    static final int STUB_ID = 1;

    static final int VALUE_FRAGMENTED = 0x40;

    // Links within usage list, guarded by Database.mUsageLatch.
    Node mMoreUsed; // points to more recently used node
    Node mLessUsed; // points to less recently used node

    // Links within dirty list, guarded by OrderedPageAllocator.
    Node mNextDirty;
    Node mPrevDirty;

    /*
      Nodes define the contents of Trees and UndoLogs. All node types start
      with a two byte header.

      +----------------------------------------+
      | byte:   node type                      |  header
      | byte:   reserved (must be 0)           |
      -                                        -

      There are two types of tree nodes, having a similar structure and
      supporting a maximum page size of 65536 bytes. The ushort type is an
      unsigned byte pair, and the ulong type is eight bytes. All multibyte
      types are little endian encoded.

      +----------------------------------------+
      | byte:   node type                      |  header
      | byte:   reserved (must be 0)           |
      | ushort: garbage in segments            |
      | ushort: pointer to left segment tail   |
      | ushort: pointer to right segment tail  |
      | ushort: pointer to search vector start |
      | ushort: pointer to search vector end   |
      +----------------------------------------+
      | left segment                           |
      -                                        -
      |                                        |
      +----------------------------------------+
      | free space                             | <-- left segment tail (exclusive)
      -                                        -
      |                                        |
      +----------------------------------------+
      | search vector                          | <-- search vector start (inclusive)
      -                                        -
      |                                        | <-- search vector end (inclusive)
      +----------------------------------------+
      | free space                             |
      -                                        -
      |                                        | <-- right segment tail (exclusive)
      +----------------------------------------+
      | right segment                          |
      -                                        -
      |                                        |
      +----------------------------------------+

      The left and right segments are used for allocating variable sized entries, and the
      tail points to the next allocation. Allocations are made toward the search vector
      such that the free space before and after the search vector remain the roughly the
      same. The search vector may move if a contiguous allocation is not possible on
      either side.

      The search vector is used for performing a binary search against keys. The keys are
      variable length and are stored anywhere in the left and right segments. The search
      vector itself must point to keys in the correct order, supporting binary search. The
      search vector is also required to be aligned to an even address, contain fixed size
      entries, and it never has holes. Adding or removing entries from the search vector
      requires entries to be shifted. The shift operation can be performed from either
      side, but the smaller shift is always chosen as a performance optimization.
      
      Garbage refers to the amount of unused bytes within the left and right allocation
      segments. Garbage accumulates when entries are deleted and updated from the
      segments. Segments are not immediately shifted because the search vector would also
      need to be repaired. A compaction operation reclaims garbage by rebuilding the
      segments and search vector. A copying garbage collection algorithm is used for this.

      The compaction implementation allocates all surviving entries in the left segment,
      leaving an empty right segment. There is no requirement that the segments be
      balanced -- this only applies to the free space surrounding the search vector.

      Leaf nodes support variable length keys and values, encoded as a pair, within the
      segments. Entries in the search vector are ushort pointers into the segments. No
      distinction is made between the segments because the pointers are absolute.

      Entries start with a one byte key header:

      0b0pxx_xxxx: key is 1..64 bytes
      0b1pxx_xxxx: key is 0..16383 bytes

      When the 'p' bit is zero, the entry is a normal key. Otherwise, it
      indicates that the key starts with the node key prefix.

      For keys 1..64 bytes in length, the length is defined as ((header & 0x3f) + 1). For
      keys 0..16383 bytes in length, a second header byte is used. The second byte is
      unsigned, and the length is defined as (((header & 0x3f) << 8) | header2). The key
      contents immediately follow the header byte(s).

      The value follows the key, and its header encodes the entry length:

      0b0xxx_xxxx: value is 0..127 bytes
      0b1f0x_xxxx: value/entry is 1..8192 bytes
      0b1f10_xxxx: value/entry is 1..1048576 bytes
      0b1111_1111: ghost value (null)

      When the 'f' bit is zero, the entry is a normal value. Otherwise, it is a
      fragmented value, defined by Database.fragment.

      For entries 1..8192 bytes in length, a second header byte is used. The
      length is then defined as ((((h0 & 0x1f) << 8) | h1) + 1). For larger
      entries, the length is ((((h0 & 0x0f) << 16) | (h1 << 8) | h2) + 1).
      Node limit is currently 65536 bytes, which limits maximum entry length.

      The "values" for internal nodes are actually identifiers for child nodes. The number
      of child nodes is always one more than the number of keys. For this reason, the
      key-value format used by leaf nodes cannot be applied to internal nodes. Also, the
      identifiers are always a fixed length, ulong type.

      Child node identifiers are encoded immediately following the search vector. Free space
      management must account for this, treating it as an extension to the search vector.

     */

    // Raw contents of node.
    byte[] mPage;

    // Id is often read without acquiring latch, although in most cases, it
    // doesn't need to be volatile. This is because a double check with the
    // latch held is always performed. So-called double-checked locking doesn't
    // work with object initialization, but it's fine with primitive types.
    // When nodes are evicted, the write operation must complete before the id
    // is re-assigned. For this reason, the id is volatile. A memory barrier
    // between the write and re-assignment should work too.
    volatile long mId;

    byte mCachedState;

    // Entries from header, available as fields for quick access.
    byte mType;
    int mGarbage;
    int mLeftSegTail;
    int mRightSegTail;
    int mSearchVecStart;
    int mSearchVecEnd;

    // References to child nodes currently available. Is null for leaf nodes.
    Node[] mChildNodes;

    // Linked stack of TreeCursorFrames bound to this Node.
    TreeCursorFrame mLastCursorFrame;

    // Set by a partially completed split.
    Split mSplit;

    Node(int pageSize) {
        mPage = new byte[pageSize];
    }

    private Node(byte[] page) {
        mPage = page;
    }

    void asEmptyRoot() {
        mId = 0;
        mCachedState = CACHED_CLEAN;
        mType = TYPE_TN_LEAF | LOW_EXTREMITY | HIGH_EXTREMITY;
        clearEntries();
    }

    private void clearEntries() {
        mGarbage = 0;
        mLeftSegTail = TN_HEADER_SIZE;
        int pageSize = mPage.length;
        mRightSegTail = pageSize - 1;
        // Search vector location must be even.
        mSearchVecStart = (TN_HEADER_SIZE + ((pageSize - TN_HEADER_SIZE) >> 1)) & ~1;
        mSearchVecEnd = mSearchVecStart - 2; // inclusive
    }

    /**
     * Caller must hold exclusive root latch and it must verify that root has split.
     *
     * @param stub Old root node stub, latched exclusively, whose cursors must
     * transfer into the new root. Stub latch is released by this method.
     */
    void finishSplitRoot(Tree tree) throws IOException {
        // Create a child node and copy this root node state into it. Then update this
        // root node to point to new and split child nodes. New root is always an internal node.

        Database db = tree.mDatabase;
        Node child = db.allocDirtyNode();

        byte[] newPage = child.mPage;
        child.mPage = mPage;
        child.mType = mType;
        child.mGarbage = mGarbage;
        child.mLeftSegTail = mLeftSegTail;
        child.mRightSegTail = mRightSegTail;
        child.mSearchVecStart = mSearchVecStart;
        child.mSearchVecEnd = mSearchVecEnd;
        child.mChildNodes = mChildNodes;
        child.mLastCursorFrame = mLastCursorFrame;

        // Fix child node cursor frame bindings.
        for (TreeCursorFrame frame = mLastCursorFrame; frame != null; ) {
            frame.mNode = child;
            frame = frame.mPrevCousin;
        }

        final Split split = mSplit;
        final Node sibling = rebindSplitFrames(split);
        mSplit = null;

        Node left, right;
        if (split.mSplitRight) {
            left = child;
            right = sibling;
        } else {
            left = sibling;
            right = child;
        }

        int leftSegTail = split.copySplitKeyToParent(newPage, TN_HEADER_SIZE);

        // Create new single-element search vector. Center it using the same formula as the
        // compactInternal method.
        final int searchVecStart = newPage.length -
            (((newPage.length - leftSegTail + (2 + 8 + 8)) >> 1) & ~1);
        encodeShortLE(newPage, searchVecStart, TN_HEADER_SIZE);
        encodeLongLE(newPage, searchVecStart + 2, left.mId);
        encodeLongLE(newPage, searchVecStart + 2 + 8, right.mId);

        // TODO: recycle these arrays
        mChildNodes = new Node[] {left, right};

        mPage = newPage;
        mType = isLeaf() ? (byte) (TYPE_TN_BIN | LOW_EXTREMITY | HIGH_EXTREMITY)
            : (byte) (TYPE_TN_IN | LOW_EXTREMITY | HIGH_EXTREMITY);
        mGarbage = 0;
        mLeftSegTail = leftSegTail;
        mRightSegTail = newPage.length - 1;
        mSearchVecStart = searchVecStart;
        mSearchVecEnd = searchVecStart;
        mLastCursorFrame = null;

        // Add a parent cursor frame for all left and right node cursors.
        addParentFrames(left, 0);
        addParentFrames(right, 2);

        child.releaseExclusive();
        sibling.releaseExclusive();

        // Split complete, so allow new node to be evictable.
        db.makeEvictable(sibling);
    }

    private void addParentFrames(Node child, int pos) {
        for (TreeCursorFrame frame = child.mLastCursorFrame; frame != null; ) {
            TreeCursorFrame parentFrame = frame.mParentFrame;
            if (parentFrame == null) {
                parentFrame = new TreeCursorFrame();
            } else {
                parentFrame.unbind();
            }
            parentFrame.bind(this, pos);
            frame.mParentFrame = parentFrame;
            frame = frame.mPrevCousin;
        }
    }

    /**
     * Caller must hold exclusive latch on node. Latch is released by this
     * method when null is returned or if an exception is thrown. If another
     * node is returned, it is latched exclusively and original is released.
     *
     * @return original or another node to be evicted; null if cannot evict
     */
    static Node evict(Node node, PageDb db) throws IOException {
        return node.mCachedState == CACHED_CLEAN ? node : null;
    }

    /**
     * Caller must hold any latch.
     */
    boolean isLeaf() {
        return mType < 0;
    }

    /**
     * Caller must hold any latch.
     */
    int numKeys() {
        return (mSearchVecEnd - mSearchVecStart + 2) >> 1;
    }

    /**
     * Caller must hold any latch.
     */
    boolean hasKeys() {
        return mSearchVecEnd >= mSearchVecStart;
    }

    /**
     * Returns the highest possible key position, which is an even number. If
     * node has no keys, return value is negative. Caller must hold any latch.
     */
    int highestKeyPos() {
        return mSearchVecEnd - mSearchVecStart;
    }

    /**
     * Returns highest leaf or internal position. Caller must hold any latch.
     */
    int highestPos() {
        int pos = mSearchVecEnd - mSearchVecStart;
        if (!isLeaf()) {
            pos += 2;
        }
        return pos;
    }

    /**
     * Returns the highest possible leaf key position, which is an even
     * number. If leaf node is empty, return value is negative. Caller must
     * hold any latch.
     */
    int highestLeafPos() {
        return mSearchVecEnd - mSearchVecStart;
    }

    /**
     * Returns the highest possible internal node position, which is an even
     * number. Highest position doesn't correspond to a valid key, but instead
     * a child node position. If internal node has no keys, node has one child
     * at position zero. Caller must hold any latch.
     */
    int highestInternalPos() {
        return mSearchVecEnd - mSearchVecStart + 2;
    }

    /**
     * Caller must hold any latch.
     */
    int availableBytes() {
        return isLeaf() ? availableLeafBytes() : availableInternalBytes();
    }

    /**
     * Caller must hold any latch.
     */
    int availableLeafBytes() {
        return mGarbage + mSearchVecStart - mSearchVecEnd
            - mLeftSegTail + mRightSegTail + (1 - 2);
    }

    /**
     * Caller must hold any latch.
     */
    int availableInternalBytes() {
        return mGarbage + 5 * (mSearchVecStart - mSearchVecEnd)
            - mLeftSegTail + mRightSegTail + (1 - (5 * 2 + 8));
    }

    /**
     * @return 2-based insertion pos, which is negative if key not found
     */
    int binarySearch(byte[] key) {
        final byte[] page = mPage;
        final int keyLen = key.length;
        int lowPos = mSearchVecStart;
        int highPos = mSearchVecEnd;

        int lowMatch = 0;
        int highMatch = 0;

        outer: while (lowPos <= highPos) {
            int midPos = ((lowPos + highPos) >> 1) & ~1;

            int compareLoc = decodeUnsignedShortLE(page, midPos);
            int compareLen = page[compareLoc++];
            compareLen = compareLen >= 0 ? ((compareLen & 0x3f) + 1)
                : (((compareLen & 0x3f) << 8) | ((page[compareLoc++]) & 0xff));

            int minLen = Math.min(compareLen, keyLen);
            int i = Math.min(lowMatch, highMatch);
            for (; i<minLen; i++) {
                byte cb = page[compareLoc + i];
                byte kb = key[i];
                if (cb != kb) {
                    if ((cb & 0xff) < (kb & 0xff)) {
                        lowPos = midPos + 2;
                        lowMatch = i;
                    } else {
                        highPos = midPos - 2;
                        highMatch = i;
                    }
                    continue outer;
                }
            }

            if (compareLen < keyLen) {
                lowPos = midPos + 2;
                lowMatch = i;
            } else if (compareLen > keyLen) {
                highPos = midPos - 2;
                highMatch = i;
            } else {
                return midPos - mSearchVecStart;
            }
        }

        return ~(lowPos - mSearchVecStart);
    }

    /**
     * Ensure binary search position is positive, for internal node.
     */
    static int internalPos(int pos) {
        return pos < 0 ? ~pos : (pos + 2);
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     */
    byte[] retrieveKey(int pos) {
        final byte[] page = mPage;
        return retrieveKeyAtLoc(page, decodeUnsignedShortLE(page, mSearchVecStart + pos));
    }

    /**
     * @param loc absolute location of entry
     */
    static byte[] retrieveKeyAtLoc(final byte[] page, int loc) {
        int keyLen = page[loc++];
        keyLen = keyLen >= 0 ? ((keyLen & 0x3f) + 1)
            : (((keyLen & 0x3f) << 8) | ((page[loc++]) & 0xff));
        byte[] key = new byte[keyLen];
        arraycopy(page, loc, key, 0, keyLen);
        return key;
    }

    /**
     * Copies the key at the given position based on a limit. If equal, the
     * limitKey instance is returned. If beyond the limit, null is returned.
     *
     * @param pos position as provided by binarySearch; must be positive
     * @param limitKey comparison key
     * @param limitMode positive for LE behavior, negative for GE behavior
     */
    byte[] retrieveKeyCmp(int pos, byte[] limitKey, int limitMode) {
        final byte[] page = mPage;
        int loc = decodeUnsignedShortLE(page, mSearchVecStart + pos);
        int keyLen = page[loc++];
        keyLen = keyLen >= 0 ? ((keyLen & 0x3f) + 1)
            : (((keyLen & 0x3f) << 8) | ((page[loc++]) & 0xff));
        int cmp = compareKeys(page, loc, keyLen, limitKey, 0, limitKey.length);
        if (cmp == 0) {
            return limitKey;
        } else if ((cmp ^ limitMode) < 0) {
            byte[] key = new byte[keyLen];
            arraycopy(page, loc, key, 0, keyLen);
            return key;
        } else {
            return null;
        }
    }

    /**
     * Used by UndoLog for decoding entries. Only works for non-fragmented values.
     *
     * @param loc absolute location of entry
     */
    static byte[][] retrieveKeyValueAtLoc(final byte[] page, int loc) throws IOException {
        int header = page[loc++];
        int keyLen = header >= 0 ? ((header & 0x3f) + 1)
            : (((header & 0x3f) << 8) | ((page[loc++]) & 0xff));
        byte[] key = new byte[keyLen];
        arraycopy(page, loc, key, 0, keyLen);
        return new byte[][] {key, retrieveLeafValueAtLoc(null, null, page, loc + keyLen)};
    }

    /**
     * Returns a new key between the low key in this node and the given high key.
     *
     * @see Utils#midKey
     */
    byte[] midKey(int lowPos, byte[] highKey) {
        final byte[] lowPage = mPage;
        int lowLoc = decodeUnsignedShortLE(lowPage, mSearchVecStart + lowPos);
        int lowKeyLen = lowPage[lowLoc++];
        lowKeyLen = lowKeyLen >= 0 ? ((lowKeyLen & 0x3f) + 1)
            : (((lowKeyLen & 0x3f) << 8) | ((lowPage[lowLoc++]) & 0xff));
        return Utils.midKey(lowPage, lowLoc, lowKeyLen, highKey, 0, highKey.length);
    }

    /**
     * Returns a new key between the given low key and the high key in this node.
     *
     * @see Utils#midKey
     */
    byte[] midKey(byte[] lowKey, int highPos) {
        final byte[] highPage = mPage;
        int highLoc = decodeUnsignedShortLE(highPage, mSearchVecStart + highPos);
        int highKeyLen = highPage[highLoc++];
        highKeyLen = highKeyLen >= 0 ? ((highKeyLen & 0x3f) + 1)
            : (((highKeyLen & 0x3f) << 8) | ((highPage[highLoc++]) & 0xff));
        return Utils.midKey(lowKey, 0, lowKey.length, highPage, highLoc, highKeyLen);
    }

    /**
     * Returns a new key between the low key in this node and the high key of another node.
     *
     * @see Utils#midKey
     */
    byte[] midKey(int lowPos, Node highNode, int highPos) {
        final byte[] lowPage = mPage;
        int lowLoc = decodeUnsignedShortLE(lowPage, mSearchVecStart + lowPos);
        int lowKeyLen = lowPage[lowLoc++];
        lowKeyLen = lowKeyLen >= 0 ? ((lowKeyLen & 0x3f) + 1)
            : (((lowKeyLen & 0x3f) << 8) | ((lowPage[lowLoc++]) & 0xff));

        final byte[] highPage = highNode.mPage;
        int highLoc = decodeUnsignedShortLE(highPage, highNode.mSearchVecStart + highPos);
        int highKeyLen = highPage[highLoc++];
        highKeyLen = highKeyLen >= 0 ? ((highKeyLen & 0x3f) + 1)
            : (((highKeyLen & 0x3f) << 8) | ((highPage[highLoc++]) & 0xff));

        return Utils.midKey(lowPage, lowLoc, lowKeyLen, highPage, highLoc, highKeyLen);
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     * @return Cursor.NOT_LOADED if value exists, null if ghost
     */
    byte[] hasLeafValue(int pos) {
        final byte[] page = mPage;
        int loc = decodeUnsignedShortLE(page, mSearchVecStart + pos);
        int header = page[loc++];
        loc += (header >= 0 ? header : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;
        return page[loc] == -1 ? null : Cursor.NOT_LOADED;
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     * @return null if ghost
     */
    byte[] retrieveLeafValue(Tree tree, int pos) throws IOException {
        final byte[] page = mPage;
        int loc = decodeUnsignedShortLE(page, mSearchVecStart + pos);
        int header = page[loc++];
        loc += (header >= 0 ? header : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;
        return retrieveLeafValueAtLoc(this, tree, page, loc);
    }

    private static byte[] retrieveLeafValueAtLoc(Node caller, Tree tree, byte[] page, int loc)
        throws IOException
    {
        final int header = page[loc++];
        if (header == 0) {
            return EMPTY_BYTES;
        }

        int len;
        if (header >= 0) {
            len = header;
        } else {
            if ((header & 0x20) == 0) {
                len = 1 + (((header & 0x1f) << 8) | (page[loc++] & 0xff));
            } else if (header != -1) {
                len = 1 + (((header & 0x0f) << 16)
                           | ((page[loc++] & 0xff) << 8) | (page[loc++] & 0xff));
            } else {
                // ghost
                return null;
            }
        }

        byte[] value = new byte[len];
        arraycopy(page, loc, value, 0, len);
        return value;
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     */
    void retrieveLeafEntry(int pos, TreeCursor cursor) throws IOException {
        final byte[] page = mPage;
        int loc = decodeUnsignedShortLE(page, mSearchVecStart + pos);
        int header = page[loc++];
        int keyLen = header >= 0 ? ((header & 0x3f) + 1)
            : (((header & 0x3f) << 8) | ((page[loc++]) & 0xff));
        byte[] key = new byte[keyLen];
        arraycopy(page, loc, key, 0, keyLen);
        cursor.mKey = key;
        cursor.mValue = retrieveLeafValueAtLoc(this, cursor.mTree, page, loc + keyLen);
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     */
    long retrieveChildRefId(int pos) {
        return decodeLongLE(mPage, mSearchVecEnd + 2 + (pos << 2));
    }

    /**
     * @param index index in child node array
     */
    long retrieveChildRefIdFromIndex(int index) {
        return decodeLongLE(mPage, mSearchVecEnd + 2 + (index << 3));
    }

    /**
     * @return length of encoded entry at given location
     */
    static int leafEntryLengthAtLoc(byte[] page, final int entryLoc) {
        int loc = entryLoc;
        int header = page[loc++];
        loc += (header >= 0 ? (header & 0x3f) : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;
        header = page[loc++];
        if (header >= 0) {
            loc += header;
        } else {
            if ((header & 0x20) == 0) {
                loc += 2 + (((header & 0x1f) << 8) | (page[loc] & 0xff));
            } else if (header != -1) {
                loc += 3 + (((header & 0x0f) << 16)
                            | ((page[loc] & 0xff) << 8) | (page[loc + 1] & 0xff));
            }
        }
        return loc - entryLoc;
    }

    /**
     * @return length of encoded key at given location, including the header
     */
    static int keyLengthAtLoc(byte[] page, final int keyLoc) {
        int header = page[keyLoc];
        return (header >= 0 ? (header & 0x3f)
                : (((header & 0x3f) << 8) | (page[keyLoc + 1] & 0xff))) + 2;
    }

    /**
     * @param pos complement of position as provided by binarySearch; must be positive
     * @param encodedKeyLen from calculateKeyLengthChecked
     */
    void insertLeafEntry(Tree tree, int pos, byte[] key, int encodedKeyLen, byte[] value)
        throws IOException
    {
        int encodedLen = encodedKeyLen + calculateLeafValueLength(value);
        int entryLoc = createLeafEntry(tree, pos, encodedLen);
        if (entryLoc < 0) {
            splitLeafAndCreateEntry(tree, key, value, encodedLen, pos, true);
        } else {
            copyToLeafEntry(key, value, entryLoc);
        }
    }

    /**
     * Verifies that key can safely fit in the node.
     */
    static int calculateKeyLengthChecked(Tree tree, byte[] key) throws LargeKeyException {
        int len = key.length;
        if (len <= 64 & len > 0) {
            // Always safe because minimum node size is 512 bytes.
            return len + 1;
        }
        if (len > tree.mMaxKeySize) {
            throw new LargeKeyException(len);
        }
        return len + 2;
    }

    /**
     * @param pos complement of position as provided by binarySearch; must be positive
     * @return Location for newly allocated entry, already pointed to by search
     * vector, or negative if leaf must be split. Complement of negative value
     * is maximum space available.
     */
    private int createLeafEntry(Tree tree, int pos, final int encodedLen) {
        int searchVecStart = mSearchVecStart;
        int searchVecEnd = mSearchVecEnd;

        int leftSpace = searchVecStart - mLeftSegTail;
        int rightSpace = mRightSegTail - searchVecEnd - 1;

        final byte[] page = mPage;

        int entryLoc;
        alloc: {
            if (pos < ((searchVecEnd - searchVecStart + 2) >> 1)) {
                // Shift subset of search vector left or prepend.
                if ((leftSpace -= 2) >= 0 &&
                    (entryLoc = allocPageEntry(encodedLen, leftSpace, rightSpace)) >= 0)
                {
                    arraycopy(page, searchVecStart, page, searchVecStart -= 2, pos);
                    pos += searchVecStart;
                    mSearchVecStart = searchVecStart;
                    break alloc;
                }
                // Need to make space, but restore leftSpace value first.
                leftSpace += 2;
            } else {
                // Shift subset of search vector right or append.
                if ((rightSpace -= 2) >= 0 &&
                    (entryLoc = allocPageEntry(encodedLen, leftSpace, rightSpace)) >= 0)
                {
                    pos += searchVecStart;
                    arraycopy(page, pos, page, pos + 2, (searchVecEnd += 2) - pos);
                    mSearchVecEnd = searchVecEnd;
                    break alloc;
                }
                // Need to make space, but restore rightSpace value first.
                rightSpace += 2;
            }

            // Compute remaining space surrounding search vector after insert completes.
            int remaining = leftSpace + rightSpace - encodedLen - 2;

            if (mGarbage > remaining) {
                compact: {
                    // Do full compaction and free up the garbage, or else node must be split.

                    if (mGarbage + remaining < 0) {
                        break compact;
                    }

                    return compactLeaf(tree, encodedLen, pos, true);
                }

                // Determine max possible entry size allowed, accounting too for entry pointer,
                // key length, and value length. Key and value length might only require only
                // require 1 byte fields, but be safe and choose the larger size of 2.
                int max = mGarbage + leftSpace + rightSpace - (2 + 2 + 2);
                return max <= 0 ? -1 : ~max;
            }

            int vecLen = searchVecEnd - searchVecStart + 2;
            int newSearchVecStart;

            if (remaining > 0 || (mRightSegTail & 1) != 0) {
                // Re-center search vector, biased to the right, ensuring proper alignment.
                newSearchVecStart = (mRightSegTail - vecLen + (1 - 2) - (remaining >> 1)) & ~1;

                // Allocate entry from left segment.
                entryLoc = mLeftSegTail;
                mLeftSegTail = entryLoc + encodedLen;
            } else if ((mLeftSegTail & 1) == 0) {
                // Move search vector left, ensuring proper alignment.
                newSearchVecStart = mLeftSegTail + ((remaining >> 1) & ~1);

                // Allocate entry from right segment.
                entryLoc = mRightSegTail - encodedLen + 1;
                mRightSegTail = entryLoc - 1;
            } else {
                // Search vector is misaligned, so do full compaction.
                return compactLeaf(tree, encodedLen, pos, true);
            }

            arrayCopies(page,
                        searchVecStart, newSearchVecStart, pos,
                        searchVecStart + pos, newSearchVecStart + pos + 2, vecLen - pos);

            pos += newSearchVecStart;
            mSearchVecStart = newSearchVecStart;
            mSearchVecEnd = newSearchVecStart + vecLen;
        }

        // Write pointer to new allocation.
        encodeShortLE(page, pos, entryLoc);
        return entryLoc;
    }

    /**
     * Insert into an internal node following a child node split. This parent node and child
     * node must have an exclusive latch held. Parent and child latch are always released, even
     * if an exception is thrown.
     *
     * @param keyPos position to insert split key
     * @param splitChild child node which split
     */
    void insertSplitChildRef(Tree tree, int keyPos, Node splitChild)
        throws IOException
    {
        final Split split = splitChild.mSplit;
        final Node newChild = splitChild.rebindSplitFrames(split);
        try {
            splitChild.mSplit = null;

            //final Node leftChild;
            final Node rightChild;
            int newChildPos = keyPos >> 1;
            if (split.mSplitRight) {
                //leftChild = splitChild;
                rightChild = newChild;
                newChildPos++;
            } else {
                //leftChild = newChild;
                rightChild = splitChild;
            }

            // Positions of frames higher than split key need to be incremented.
            for (TreeCursorFrame frame = mLastCursorFrame; frame != null; ) {
                int framePos = frame.mNodePos;
                if (framePos > keyPos) {
                    frame.mNodePos = framePos + 2;
                }
                frame = frame.mPrevCousin;
            }

            // Positions of frames equal to split key are in the split itself. Only
            // frames for the right split need to be incremented.
            for (TreeCursorFrame childFrame = rightChild.mLastCursorFrame; childFrame != null; ) {
                TreeCursorFrame frame = childFrame.mParentFrame;
                if (frame.mNode != this) {
                    throw new AssertionError("Invalid cursor frame parent");
                }
                frame.mNodePos += 2;
                childFrame = childFrame.mPrevCousin;
            }

            // Update references to child node instances.
            {
                // TODO: recycle child node arrays
                Node[] newChildNodes = new Node[mChildNodes.length + 1];
                arraycopy(mChildNodes, 0, newChildNodes, 0, newChildPos);
                arraycopy(mChildNodes, newChildPos, newChildNodes, newChildPos + 1,
                          mChildNodes.length - newChildPos);
                newChildNodes[newChildPos] = newChild;
                mChildNodes = newChildNodes;

                // Rescale for long ids as encoded in page.
                newChildPos <<= 3;
            }

            // FIXME: IOException; how to rollback the damage?
            InResult result = createInternalEntry
                (tree, keyPos, split.splitKeyEncodedLength(), newChildPos, true);

            // Write new child id.
            encodeLongLE(result.mPage, result.mNewChildLoc, newChild.mId);

            int entryLoc = result.mEntryLoc;
            if (entryLoc < 0) {
                // If loc is negative, then node was split and new key was chosen to be promoted.
                // It must be written into the new split.
                mSplit.setKey(split);
            } else {
                // Write key entry itself.
                split.copySplitKeyToParent(result.mPage, entryLoc);
            }
        } catch (Throwable e) {
            splitChild.releaseExclusive();
            newChild.releaseExclusive();
            releaseExclusive();
            throw e;
        }
        
        splitChild.releaseExclusive();
        newChild.releaseExclusive();

        try {
            // Split complete, so allow new node to be evictable.
            tree.mDatabase.makeEvictable(newChild);
        } catch (Throwable e) {
            releaseExclusive();
            throw e;
        }
    }

    /**
     * Insert into an internal node following a child node split. This parent
     * node and child node must have an exclusive latch held. Child latch is
     * released, unless an exception is thrown.
     *
     * @param keyPos 2-based position
     * @param newChildPos 8-based position
     * @param allowSplit true if this internal node can be split as a side-effect
     * @return result; if node was split, key and entry loc is -1 if new key was promoted to parent
     * @throws AssertionError if entry must be split to make room but split is not allowed
     */
    private InResult createInternalEntry(Tree tree, int keyPos, int encodedLen,
                                         int newChildPos, boolean allowSplit)
        throws IOException
    {
        int searchVecStart = mSearchVecStart;
        int searchVecEnd = mSearchVecEnd;

        int leftSpace = searchVecStart - mLeftSegTail;
        int rightSpace = mRightSegTail - searchVecEnd
            - ((searchVecEnd - searchVecStart) << 2) - 17;

        byte[] page = mPage;

        int entryLoc;
        alloc: {
            // Need to make room for one new search vector entry (2 bytes) and one new child
            // id entry (8 bytes). Determine which shift operations minimize movement.
            if (newChildPos < ((3 * (searchVecEnd - searchVecStart + 2) + keyPos + 8) >> 1)) {
                // Attempt to shift search vector left by 10, shift child ids left by 8.

                if ((leftSpace -= 10) >= 0 &&
                    (entryLoc = allocPageEntry(encodedLen, leftSpace, rightSpace)) >= 0)
                {
                    arraycopy(page, searchVecStart, page, searchVecStart - 10, keyPos);
                    arraycopy(page, searchVecStart + keyPos,
                              page, searchVecStart + keyPos - 8,
                              searchVecEnd - searchVecStart + 2 - keyPos + newChildPos);
                    mSearchVecStart = searchVecStart -= 10;
                    keyPos += searchVecStart;
                    mSearchVecEnd = searchVecEnd -= 8;
                    newChildPos += searchVecEnd + 2;
                    break alloc;
                }

                // Need to make space, but restore leftSpace value first.
                leftSpace += 10;
            } else {
                // Attempt to shift search vector left by 2, shift child ids right by 8.

                leftSpace -= 2;
                rightSpace -= 8;

                if (leftSpace >= 0 && rightSpace >= 0 &&
                    (entryLoc = allocPageEntry(encodedLen, leftSpace, rightSpace)) >= 0)
                {
                    arraycopy(page, searchVecStart, page, searchVecStart -= 2, keyPos);
                    mSearchVecStart = searchVecStart;
                    keyPos += searchVecStart;
                    arraycopy(page, searchVecEnd + newChildPos + 2,
                              page, searchVecEnd + newChildPos + (2 + 8),
                              ((searchVecEnd - searchVecStart) << 2) + 8 - newChildPos);
                    newChildPos += searchVecEnd + 2;
                    break alloc;
                }

                // Need to make space, but restore space values first.
                leftSpace += 2;
                rightSpace += 8;
            }

            // Compute remaining space surrounding search vector after insert completes.
            int remaining = leftSpace + rightSpace - encodedLen - 10;

            if (mGarbage > remaining) {
                compact: {
                    // Do full compaction and free up the garbage, or else node must be split.

                    if ((mGarbage + remaining) < 0) {
                        break compact;
                    }

                    return compactInternal(tree, encodedLen, keyPos, newChildPos);
                }

                // Node is full, so split it.

                if (!allowSplit) {
                    throw new AssertionError("Split not allowed");
                }

                // No side-effects if an IOException is thrown here.
                return splitInternal(tree, encodedLen, keyPos, newChildPos);
            }

            int vecLen = searchVecEnd - searchVecStart + 2;
            int childIdsLen = (vecLen << 2) + 8;
            int newSearchVecStart;

            if (remaining > 0 || (mRightSegTail & 1) != 0) {
                // Re-center search vector, biased to the right, ensuring proper alignment.
                newSearchVecStart =
                    (mRightSegTail - vecLen - childIdsLen + (1 - 10) - (remaining >> 1)) & ~1;

                // Allocate entry from left segment.
                entryLoc = mLeftSegTail;
                mLeftSegTail = entryLoc + encodedLen;
            } else if ((mLeftSegTail & 1) == 0) {
                // Move search vector left, ensuring proper alignment.
                newSearchVecStart = mLeftSegTail + ((remaining >> 1) & ~1);

                // Allocate entry from right segment.
                entryLoc = mRightSegTail - encodedLen + 1;
                mRightSegTail = entryLoc - 1;
            } else {
                // Search vector is misaligned, so do full compaction.
                return compactInternal(tree, encodedLen, keyPos, newChildPos);
            }

            int newSearchVecEnd = newSearchVecStart + vecLen;

            arrayCopies(page,
                        // Move search vector up to new key position.
                        searchVecStart, newSearchVecStart, keyPos,

                        // Move search vector after new key position, to new child
                        // id position.
                        searchVecStart + keyPos,
                        newSearchVecStart + keyPos + 2,
                        vecLen - keyPos + newChildPos,

                        // Move search vector after new child id position.
                        searchVecEnd + 2 + newChildPos,
                        newSearchVecEnd + 10 + newChildPos,
                        childIdsLen - newChildPos);

            keyPos += newSearchVecStart;
            newChildPos += newSearchVecEnd + 2;
            mSearchVecStart = newSearchVecStart;
            mSearchVecEnd = newSearchVecEnd;
        }

        // Write pointer to key entry.
        encodeShortLE(page, keyPos, entryLoc);

        InResult result = new InResult();
        result.mPage = page;
        result.mKeyLoc = keyPos;
        result.mNewChildLoc = newChildPos;
        result.mEntryLoc = entryLoc;

        return result;
    }

    /**
     * Rebind cursor frames affected by split to correct node and
     * position. Caller must hold exclusive latch.
     *
     * @return latched sibling
     */
    private Node rebindSplitFrames(Split split) {
        final Node sibling = split.latchSibling();
        try {
            for (TreeCursorFrame frame = mLastCursorFrame; frame != null; ) {
                // Capture previous frame from linked list before changing the links.
                TreeCursorFrame prev = frame.mPrevCousin;
                split.rebindFrame(frame, sibling);
                frame = prev;
            }
            return sibling;
        } catch (Throwable e) {
            sibling.releaseExclusive();
            throw e;
        }
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     */
    void updateChildRefId(int pos, long id) {
        encodeLongLE(mPage, mSearchVecEnd + 2 + (pos << 2), id);
    }

    /**
     * Calculate encoded key length, including header.
     */
    static int calculateKeyLength(byte[] key) {
        int len = key.length;
        return len + ((len <= 64 & len > 0) ? 1 : 2);
    }

    /**
     * Calculate encoded value length for leaf, including header.
     */
    private static int calculateLeafValueLength(byte[] value) {
        int len = value.length;
        return len + ((len <= 127) ? 1 : ((len <= 8192) ? 2 : 3));
    }

    /**
     * Calculate encoded value length for leaf, including header.
     */
    private static long calculateLeafValueLength(long vlength) {
        return vlength + ((vlength <= 127) ? 1 : ((vlength <= 8192) ? 2 : 3));
    }

    /**
     * @param key unencoded key
     * @param dest destination for encoded key, with room for key header
     * @return updated destLoc
     */
    static int encodeKey(final byte[] key, final byte[] dest, int destLoc) {
        final int keyLen = key.length;

        if (keyLen <= 64 && keyLen > 0) {
            dest[destLoc++] = (byte) (keyLen - 1);
        } else {
            dest[destLoc++] = (byte) (0x80 | (keyLen >> 8));
            dest[destLoc++] = (byte) keyLen;
        }
        arraycopy(key, 0, dest, destLoc, keyLen);

        return destLoc + keyLen;
    }

    /**
     * @return -1 if not enough contiguous space surrounding search vector
     */
    private int allocPageEntry(int encodedLen, int leftSpace, int rightSpace) {
        final int entryLoc;
        if (encodedLen <= leftSpace && leftSpace >= rightSpace) {
            // Allocate entry from left segment.
            entryLoc = mLeftSegTail;
            mLeftSegTail = entryLoc + encodedLen;
        } else if (encodedLen <= rightSpace) {
            // Allocate entry from right segment.
            entryLoc = mRightSegTail - encodedLen + 1;
            mRightSegTail = entryLoc - 1;
        } else {
            // No room.
            return -1;
        }
        return entryLoc;
    }

    private void copyToLeafEntry(byte[] key, byte[] value, int entryLoc) {
        final byte[] page = mPage;

        final int len = key.length;
        if (len <= 64 && len > 0) {
            page[entryLoc++] = (byte) (len - 1);
        } else {
            page[entryLoc++] = (byte) (0x80 | (len >> 8));
            page[entryLoc++] = (byte) len;
        }
        arraycopy(key, 0, page, entryLoc, len);

        copyToLeafValue(page, 0, value, entryLoc + len);
    }

    /**
     * @param fragmented 0 or VALUE_FRAGMENTED
     * @return page location for first byte of value (first location after header)
     */
    private static int copyToLeafValue(byte[] page, int fragmented, byte[] value, int valueLoc) {
        final int len = value.length;
        if (len <= 127 && fragmented == 0) {
            page[valueLoc++] = (byte) len;
        } else if (len <= 8192) {
            page[valueLoc++] = (byte) (0x80 | fragmented | ((len - 1) >> 8));
            page[valueLoc++] = (byte) (len - 1);
        } else {
            page[valueLoc++] = (byte) (0xa0 | fragmented | ((len - 1) >> 16));
            page[valueLoc++] = (byte) ((len - 1) >> 8);
            page[valueLoc++] = (byte) (len - 1);
        }
        arraycopy(value, 0, page, valueLoc, len);
        return valueLoc;
    }

    /**
     * Compact leaf by reclaiming garbage and moving search vector towards
     * tail. Caller is responsible for ensuring that new entry will fit after
     * compaction. Space is allocated for new entry, and the search vector
     * points to it.
     *
     * @param encodedLen length of new entry to allocate
     * @param pos normalized search vector position of entry to insert/update
     * @return location for newly allocated entry, already pointed to by search vector
     */
    private int compactLeaf(Tree tree, int encodedLen, int pos, boolean forInsert) {
        byte[] page = mPage;

        int searchVecLoc = mSearchVecStart;
        // Size of search vector, possibly with new entry.
        int newSearchVecSize = mSearchVecEnd - searchVecLoc + 2;
        if (forInsert) {
            newSearchVecSize += 2;
        }
        pos += searchVecLoc;

        // Determine new location of search vector, with room to grow on both ends.
        int newSearchVecStart;
        // Capacity available to search vector after compaction.
        int searchVecCap = mGarbage + mRightSegTail + 1 - mLeftSegTail - encodedLen;
        newSearchVecStart = page.length - (((searchVecCap + newSearchVecSize) >> 1) & ~1);

        // Copy into a fresh buffer.

        int destLoc = TN_HEADER_SIZE;
        int newSearchVecLoc = newSearchVecStart;
        int newLoc = 0;
        final int searchVecEnd = mSearchVecEnd;

        Database db = tree.mDatabase;
        byte[] dest = db.removeSpareBuffer();

        for (; searchVecLoc <= searchVecEnd; searchVecLoc += 2, newSearchVecLoc += 2) {
            if (searchVecLoc == pos) {
                newLoc = newSearchVecLoc;
                if (forInsert) {
                    newSearchVecLoc += 2;
                } else {
                    continue;
                }
            }
            encodeShortLE(dest, newSearchVecLoc, destLoc);
            int sourceLoc = decodeUnsignedShortLE(page, searchVecLoc);
            int len = leafEntryLengthAtLoc(page, sourceLoc);
            arraycopy(page, sourceLoc, dest, destLoc, len);
            destLoc += len;
        }

        // Recycle old page buffer.
        db.addSpareBuffer(page);

        // Write pointer to new allocation.
        encodeShortLE(dest, newLoc == 0 ? newSearchVecLoc : newLoc, destLoc);

        mPage = dest;
        mGarbage = 0;
        mLeftSegTail = destLoc + encodedLen;
        mRightSegTail = dest.length - 1;
        mSearchVecStart = newSearchVecStart;
        mSearchVecEnd = newSearchVecStart + newSearchVecSize - 2;

        return destLoc;
    }

    /**
     * @param encodedLen length of new entry to allocate
     * @param pos normalized search vector position of entry to insert/update
     */
    private void splitLeafAndCreateEntry(Tree tree, byte[] key, byte[] value,
                                         int encodedLen, int pos, boolean forInsert)
        throws IOException
    {
        if (mSplit != null) {
            throw new AssertionError("Node is already split");
        }

        // Split can move node entries to a new left or right node. Choose such that the
        // new entry is more likely to go into the new node. This distributes the cost of
        // the split by postponing compaction of this node.

        // Since the split key and final node sizes are not known in advance, don't
        // attempt to properly center the new search vector. Instead, minimize
        // fragmentation to ensure that split is successful.

        byte[] page = mPage;

        Node newNode = tree.mDatabase.allocUnevictableNode();
        newNode.mGarbage = 0;

        byte[] newPage = newNode.mPage;

        if (forInsert && pos == 0) {
            // Inserting into left edge of node, possibly because inserts are
            // descending. Split into new left node, but only the new entry
            // goes into the new node.

            mSplit = newSplitLeft(newNode);
            // Choose an appropriate middle key for suffix compression.
            mSplit.setKey(midKey(key, 0));

            // Position search vector at extreme left, allowing new entries to
            // be placed in a natural descending order.
            newNode.mLeftSegTail = TN_HEADER_SIZE;
            newNode.mSearchVecStart = TN_HEADER_SIZE;
            newNode.mSearchVecEnd = TN_HEADER_SIZE;

            int destLoc = newPage.length - encodedLen;
            newNode.copyToLeafEntry(key, value, destLoc);
            encodeShortLE(newPage, TN_HEADER_SIZE, destLoc);

            newNode.mRightSegTail = destLoc - 1;
            newNode.releaseExclusive();

            return;
        }

        final int searchVecStart = mSearchVecStart;
        final int searchVecEnd = mSearchVecEnd;

        pos += searchVecStart;

        if (forInsert && pos == searchVecEnd + 2) {
            // Inserting into right edge of node, possibly because inserts are
            // ascending. Split into new right node, but only the new entry
            // goes into the new node.

            mSplit = newSplitRight(newNode);
            // Choose an appropriate middle key for suffix compression.
            mSplit.setKey(midKey(pos - searchVecStart - 2, key));

            // Position search vector at extreme right, allowing new entries to
            // be placed in a natural ascending order.
            newNode.mRightSegTail = newPage.length - 1;
            newNode.mSearchVecStart =
                newNode.mSearchVecEnd = newPage.length - 2;

            newNode.copyToLeafEntry(key, value, TN_HEADER_SIZE);
            encodeShortLE(newPage, newPage.length - 2, TN_HEADER_SIZE);

            newNode.mLeftSegTail = TN_HEADER_SIZE + encodedLen;
            newNode.releaseExclusive();

            return;
        }

        // Amount of bytes available in unsplit node.
        int avail = availableLeafBytes();

        int garbageAccum = 0;
        int newLoc = 0;
        int newAvail = newPage.length - TN_HEADER_SIZE;

        // Guess which way to split by examining search position. This doesn't take into
        // consideration the variable size of the entries. If the guess is wrong, the new
        // entry is inserted into original node, which now has space.

        if ((pos - searchVecStart) < (searchVecEnd - pos)) {
            // Split into new left node.

            int destLoc = newPage.length;
            int newSearchVecLoc = TN_HEADER_SIZE;

            int searchVecLoc = searchVecStart;
            for (; newAvail > avail; searchVecLoc += 2, newSearchVecLoc += 2) {
                int entryLoc = decodeUnsignedShortLE(page, searchVecLoc);
                int entryLen = leafEntryLengthAtLoc(page, entryLoc);

                if (searchVecLoc == pos) {
                    if ((newAvail -= encodedLen + 2) < 0) {
                        // Entry doesn't fit into new node.
                        break;
                    }
                    newLoc = newSearchVecLoc;
                    if (forInsert) {
                        // Reserve slot in vector for new entry.
                        newSearchVecLoc += 2;
                        if (newAvail <= avail) {
                            // Balanced enough.
                            break;
                        }
                    } else {
                        // Don't copy old entry.
                        garbageAccum += entryLen;
                        avail += entryLen;
                        continue;
                    }
                }

                if ((newAvail -= entryLen + 2) < 0) {
                    // Entry doesn't fit into new node.
                    break;
                }

                // Copy entry and point to it.
                destLoc -= entryLen;
                arraycopy(page, entryLoc, newPage, destLoc, entryLen);
                encodeShortLE(newPage, newSearchVecLoc, destLoc);

                garbageAccum += entryLen;
                avail += entryLen + 2;
            }

            // Allocate Split object first, in case it throws an OutOfMemoryError.
            mSplit = newSplitLeft(newNode);

            // Prune off the left end of this node.
            mSearchVecStart = searchVecLoc;
            mGarbage += garbageAccum;

            newNode.mLeftSegTail = TN_HEADER_SIZE;
            newNode.mSearchVecStart = TN_HEADER_SIZE;
            newNode.mSearchVecEnd = newSearchVecLoc - 2;

            try {
                if (newLoc == 0) {
                    // Unable to insert new entry into left node. Insert it
                    // into the right node, which should have space now.
                    storeIntoSplitLeaf(tree, key, value, encodedLen, forInsert);
                } else {
                    // Create new entry and point to it.
                    destLoc -= encodedLen;
                    newNode.copyToLeafEntry(key, value, destLoc);
                    encodeShortLE(newPage, newLoc, destLoc);
                }
            } finally {
                // Choose an appropriate middle key for suffix compression.
                mSplit.setKey(newNode.midKey(newNode.highestKeyPos(), this, 0));
                newNode.mRightSegTail = destLoc - 1;
                newNode.releaseExclusive();
            }
        } else {
            // Split into new right node.

            int destLoc = TN_HEADER_SIZE;
            int newSearchVecLoc = newPage.length - 2;

            int searchVecLoc = searchVecEnd;
            for (; newAvail > avail; searchVecLoc -= 2, newSearchVecLoc -= 2) {
                int entryLoc = decodeUnsignedShortLE(page, searchVecLoc);
                int entryLen = leafEntryLengthAtLoc(page, entryLoc);

                if (forInsert) {
                    if (searchVecLoc + 2 == pos) {
                        if ((newAvail -= encodedLen + 2) < 0) {
                            // Inserted entry doesn't fit into new node.
                            break;
                        }
                        // Reserve spot in vector for new entry.
                        newLoc = newSearchVecLoc;
                        newSearchVecLoc -= 2;
                        if (newAvail <= avail) {
                            // Balanced enough.
                            break;
                        }
                    }
                } else {
                    if (searchVecLoc == pos) {
                        if ((newAvail -= encodedLen + 2) < 0) {
                            // Updated entry doesn't fit into new node.
                            break;
                        }
                        // Don't copy old entry.
                        newLoc = newSearchVecLoc;
                        garbageAccum += entryLen;
                        avail += entryLen;
                        continue;
                    }
                }

                if ((newAvail -= entryLen + 2) < 0) {
                    // Entry doesn't fit into new node.
                    break;
                }

                // Copy entry and point to it.
                arraycopy(page, entryLoc, newPage, destLoc, entryLen);
                encodeShortLE(newPage, newSearchVecLoc, destLoc);
                destLoc += entryLen;

                garbageAccum += entryLen;
                avail += entryLen + 2;
            }

            // Allocate Split object first, in case it throws an OutOfMemoryError.
            mSplit = newSplitRight(newNode);

            // Prune off the right end of this node.
            mSearchVecEnd = searchVecLoc;
            mGarbage += garbageAccum;

            newNode.mRightSegTail = newPage.length - 1;
            newNode.mSearchVecStart = newSearchVecLoc + 2;
            newNode.mSearchVecEnd = newPage.length - 2;

            try {
                if (newLoc == 0) {
                    // Unable to insert new entry into new right node. Insert
                    // it into the left node, which should have space now.
                    storeIntoSplitLeaf(tree, key, value, encodedLen, forInsert);
                } else {
                    // Create new entry and point to it.
                    newNode.copyToLeafEntry(key, value, destLoc);
                    encodeShortLE(newPage, newLoc, destLoc);
                    destLoc += encodedLen;
                }
            } finally {
                // Choose an appropriate middle key for suffix compression.
                mSplit.setKey(this.midKey(this.highestKeyPos(), newNode, 0));
                newNode.mLeftSegTail = destLoc;
                newNode.releaseExclusive();
            }
        }
    }

    /**
     * Store an entry into a node which has just been split and has room.
     */
    private void storeIntoSplitLeaf(Tree tree, byte[] key, byte[] value,
                                    int encodedLen, boolean forInsert)
        throws IOException
    {
        int pos = binarySearch(key);
        if (forInsert) {
            if (pos >= 0) {
                throw new AssertionError("Key exists");
            }
            int entryLoc = createLeafEntry(tree, ~pos, encodedLen);
            copyToLeafEntry(key, value, entryLoc);
        }
    }

    /**
     * @throws IOException if new node could not be allocated; no side-effects
     * @return split result; key and entry loc is -1 if new key was promoted to parent
     */
    private InResult splitInternal
        (final Tree tree, final int encodedLen, final int keyPos, final int newChildPos)
        throws IOException
    {
        if (mSplit != null) {
            throw new AssertionError("Node is already split");
        }

        // Split can move node entries to a new left or right node. Choose such that the
        // new entry is more likely to go into the new node. This distributes the cost of
        // the split by postponing compaction of this node.

        final byte[] page = mPage;

        // Alloc early in case an exception is thrown.
        final Node newNode = tree.mDatabase.allocUnevictableNode();
        newNode.mGarbage = 0;

        final byte[] newPage = newNode.mPage;

        final InResult result = new InResult();

        final int searchVecStart = mSearchVecStart;
        final int searchVecEnd = mSearchVecEnd;

        if ((searchVecEnd - searchVecStart) == 2 && keyPos == 2) {
            // Node has two keys and the key to insert should go in the middle. The new key
            // should not be inserted, but instead be promoted to the parent. Treat this as a
            // special case -- the code below only promotes an existing key to the parent.
            // This case is expected to only occur when using very large keys.

            // Signals that key should not be inserted.
            result.mKeyLoc = -1;
            result.mEntryLoc = -1;

            int leftKeyLoc = decodeUnsignedShortLE(page, searchVecStart);
            int leftKeyLen = keyLengthAtLoc(page, leftKeyLoc);

            // Assume a large key will be inserted later, so arrange it with room: entry at far
            // left and search vector at far right.
            arraycopy(page, leftKeyLoc, newPage, TN_HEADER_SIZE, leftKeyLen);
            int leftSearchVecStart = newPage.length - (2 + 8 + 8);
            encodeShortLE(newPage, leftSearchVecStart, TN_HEADER_SIZE);

            if (newChildPos == 8) {
                // Caller must store child id into left node.
                result.mPage = newPage;
                result.mNewChildLoc = leftSearchVecStart + (2 + 8);
            } else {
                if (newChildPos != 16) {
                    throw new AssertionError();
                }
                // Caller must store child id into right node.
                result.mPage = page;
                result.mNewChildLoc = searchVecEnd + (2 + 8);
            }

            // Copy one or two left existing child ids to left node (newChildPos is 8 or 16).
            arraycopy(page, searchVecEnd + 2, newPage, leftSearchVecStart + 2, newChildPos);

            // Split references to child node instances. New child node has already
            // been placed into mChildNodes by caller.
            // TODO: recycle child node arrays
            newNode.mChildNodes = new Node[] {mChildNodes[0], mChildNodes[1]};
            mChildNodes = new Node[] {mChildNodes[2], mChildNodes[3]};

            newNode.mLeftSegTail = TN_HEADER_SIZE + leftKeyLen;
            newNode.mRightSegTail = leftSearchVecStart + (2 + 8 + 8 - 1);
            newNode.mSearchVecStart = leftSearchVecStart;
            newNode.mSearchVecEnd = leftSearchVecStart;
            newNode.releaseExclusive();

            // Prune off the left end of this node by shifting vector towards child ids.
            arraycopy(page, searchVecEnd, page, searchVecEnd + 8, 2);
            mSearchVecStart = mSearchVecEnd = searchVecEnd + 8;

            mGarbage += leftKeyLen;

            // Caller must set the split key.
            mSplit = newSplitLeft(newNode);

            return result;
        }

        result.mPage = newPage;
        final int keyLoc = keyPos + searchVecStart;

        int garbageAccum;
        int newKeyLoc;

        // Guess which way to split by examining search position. This doesn't take into
        // consideration the variable size of the entries. If the guess is wrong, do over
        // the other way. Internal splits are infrequent, and split guesses are usually
        // correct. For these reasons, it isn't worth the trouble to create a special case
        // to charge ahead with the wrong guess. Leaf node splits are more frequent, and
        // incorrect guesses are easily corrected due to the simpler leaf node structure.

        // -2: left
        // -1: guess left
        // +1: guess right
        // +2: right
        int splitSide = (keyPos < (searchVecEnd - searchVecStart - keyPos)) ? -1 : 1;

        Split split;
        doSplit: while (true) {
            garbageAccum = 0;
            newKeyLoc = 0;

            // Amount of bytes used in unsplit node, including the page header.
            int size = 5 * (searchVecEnd - searchVecStart) + (1 + 8 + 8)
                + mLeftSegTail + page.length - mRightSegTail - mGarbage;

            int newSize = TN_HEADER_SIZE;

            // Adjust sizes for extra child id -- always one more than number of keys.
            size -= 8;
            newSize += 8;

            if (splitSide < 0) {
                // Split into new left node.

                // Since the split key and final node sizes are not known in advance,
                // don't attempt to properly center the new search vector. Instead,
                // minimize fragmentation to ensure that split is successful.

                int destLoc = newPage.length;
                int newSearchVecLoc = TN_HEADER_SIZE;

                int searchVecLoc = searchVecStart;
                while (true) {
                    if (searchVecLoc == keyLoc) {
                        newKeyLoc = newSearchVecLoc;
                        newSearchVecLoc += 2;
                        // Reserve slot in vector for new entry and account for size increase.
                        newSize += encodedLen + (2 + 8);
                    }

                    int entryLoc = decodeUnsignedShortLE(page, searchVecLoc);
                    int entryLen = keyLengthAtLoc(page, entryLoc);

                    // Size change must incorporate child id, although they are copied later.
                    int sizeChange = entryLen + (2 + 8);
                    size -= sizeChange;
                    newSize += sizeChange;

                    sizeCheck: {
                        if (size <= TN_HEADER_SIZE || newSize >= newPage.length) {
                            // Moved too many entries to new node, so undo. Code can probably
                            // be written such that undo is not required, but this case is only
                            // expected to occur when using large keys.
                            if (searchVecLoc == keyLoc) {
                                // New entry doesn't fit.
                                newKeyLoc = 0;
                            }
                            newSearchVecLoc -= 2;
                            entryLoc = decodeUnsignedShortLE(page, searchVecLoc - 2);
                            entryLen = keyLengthAtLoc(page, entryLoc);
                            destLoc += entryLen;
                        } else {
                            searchVecLoc += 2;

                            // Note that last examined key is not moved but instead
                            // dropped. Garbage must account for this.
                            garbageAccum += entryLen;

                            if (newSize < size) {
                                // Keep moving entries until balanced.
                                break sizeCheck;
                            }
                        }

                        // New node has accumlated enough entries...

                        if (newKeyLoc != 0) {
                            // ...and split key has been found.
                            split = newSplitLeft(newNode);
                            split.setKey(retrieveKeyAtLoc(page, entryLoc));
                            break;
                        }

                        if (splitSide == -1) {
                            // Guessed wrong; do over on right side.
                            splitSide = 2;
                            continue doSplit;
                        }

                        // Keep searching on this side for new entry location.
                        if (splitSide != -2) {
                            throw new AssertionError();
                        }
                    }

                    // Copy key entry and point to it.
                    destLoc -= entryLen;
                    arraycopy(page, entryLoc, newPage, destLoc, entryLen);
                    encodeShortLE(newPage, newSearchVecLoc, destLoc);
                    newSearchVecLoc += 2;
                }

                result.mEntryLoc = destLoc - encodedLen;

                // Copy existing child ids and insert new child id.
                {
                    arraycopy(page, searchVecEnd + 2,
                              newPage, newSearchVecLoc, newChildPos);

                    // Leave gap for new child id, to be set by caller.
                    result.mNewChildLoc = newSearchVecLoc + newChildPos;

                    int tailChildIdsLen = ((searchVecLoc - searchVecStart) << 2) - newChildPos;
                    arraycopy(page, searchVecEnd + 2 + newChildPos,
                              newPage, newSearchVecLoc + newChildPos + 8, tailChildIdsLen);

                    // Split references to child node instances. New child node has already
                    // been placed into mChildNodes by caller.
                    // TODO: recycle child node arrays
                    int leftLen = ((newSearchVecLoc - TN_HEADER_SIZE) >> 1) + 1;
                    Node[] leftChildNodes = new Node[leftLen];
                    Node[] rightChildNodes = new Node[mChildNodes.length - leftLen];
                    arraycopy(mChildNodes, 0, leftChildNodes, 0, leftLen);
                    arraycopy(mChildNodes, leftLen,
                              rightChildNodes, 0, rightChildNodes.length);
                    newNode.mChildNodes = leftChildNodes;
                    mChildNodes = rightChildNodes;
                }

                newNode.mLeftSegTail = TN_HEADER_SIZE;
                newNode.mRightSegTail = destLoc - encodedLen - 1;
                newNode.mSearchVecStart = TN_HEADER_SIZE;
                newNode.mSearchVecEnd = newSearchVecLoc - 2;
                newNode.releaseExclusive();

                // Prune off the left end of this node by shifting vector towards child ids.
                int shift = (searchVecLoc - searchVecStart) << 2;
                int len = searchVecEnd - searchVecLoc + 2;
                arraycopy(page, searchVecLoc,
                          page, mSearchVecStart = searchVecLoc + shift, len);
                mSearchVecEnd = searchVecEnd + shift;
            } else {
                // Split into new right node.

                // First copy keys and not the child ids. After keys are copied, shift to
                // make room for child ids and copy them in place.

                int destLoc = TN_HEADER_SIZE;
                int newSearchVecLoc = newPage.length;

                int searchVecLoc = searchVecEnd + 2;
                moveEntries: while (true) {
                    if (searchVecLoc == keyLoc) {
                        newSearchVecLoc -= 2;
                        newKeyLoc = newSearchVecLoc;
                        // Reserve slot in vector for new entry and account for size increase.
                        newSize += encodedLen + (2 + 8);
                    }

                    searchVecLoc -= 2;

                    int entryLoc = decodeUnsignedShortLE(page, searchVecLoc);
                    int entryLen = keyLengthAtLoc(page, entryLoc);

                    // Size change must incorporate child id, although they are copied later.
                    int sizeChange = entryLen + (2 + 8);
                    size -= sizeChange;
                    newSize += sizeChange;

                    sizeCheck: {
                        if (size <= TN_HEADER_SIZE || newSize >= newPage.length) {
                            // Moved too many entries to new node, so undo. Code can probably
                            // be written such that undo is not required, but this case is only
                            // expected to occur when using large keys.
                            searchVecLoc += 2;
                            if (searchVecLoc == keyLoc) {
                                // New entry doesn't fit.
                                newKeyLoc = 0;
                            }
                            newSearchVecLoc += 2;
                            entryLoc = decodeUnsignedShortLE(page, searchVecLoc);
                            entryLen = keyLengthAtLoc(page, entryLoc);
                            destLoc -= entryLen;
                        } else {
                            // Note that last examined key is not moved but instead
                            // dropped. Garbage must account for this.
                            garbageAccum += entryLen;

                            if (newSize < size) {
                                // Keep moving entries until balanced.
                                break sizeCheck;
                            }
                        }

                        // New node has accumlated enough entries...

                        if (newKeyLoc != 0) {
                            // ...and split key has been found.
                            split = newSplitRight(newNode);
                            split.setKey(retrieveKeyAtLoc(page, entryLoc));
                            break moveEntries;
                        }

                        if (splitSide == 1) {
                            // Guessed wrong; do over on left side.
                            splitSide = -2;
                            continue doSplit;
                        }

                        // Keep searching on this side for new entry location.
                        if (splitSide != 2) {
                            throw new AssertionError();
                        }
                    }

                    // Copy key entry and point to it.
                    arraycopy(page, entryLoc, newPage, destLoc, entryLen);
                    newSearchVecLoc -= 2;
                    encodeShortLE(newPage, newSearchVecLoc, destLoc);
                    destLoc += entryLen;
                }

                result.mEntryLoc = destLoc;

                // Move new search vector to make room for child ids and be centered between
                // the segments.
                int newVecLen = page.length - newSearchVecLoc;
                {
                    int highestLoc = newPage.length - (5 * newVecLen) - 8;
                    int midLoc = ((destLoc + encodedLen + highestLoc + 1) >> 1) & ~1;
                    arraycopy(newPage, newSearchVecLoc, newPage, midLoc, newVecLen);
                    newKeyLoc -= newSearchVecLoc - midLoc;
                    newSearchVecLoc = midLoc;
                }

                int newSearchVecEnd = newSearchVecLoc + newVecLen - 2;

                // Copy existing child ids and insert new child id.
                {
                    int headChildIdsLen = newChildPos - ((searchVecLoc - searchVecStart + 2) << 2);
                    int newDestLoc = newSearchVecEnd + 2;
                    arraycopy(page, searchVecEnd + 2 + newChildPos - headChildIdsLen,
                              newPage, newDestLoc, headChildIdsLen);

                    // Leave gap for new child id, to be set by caller.
                    newDestLoc += headChildIdsLen;
                    result.mNewChildLoc = newDestLoc;

                    int tailChildIdsLen =
                        ((searchVecEnd - searchVecStart) << 2) + 16 - newChildPos;
                    arraycopy(page, searchVecEnd + 2 + newChildPos,
                              newPage, newDestLoc + 8, tailChildIdsLen);

                    // Split references to child node instances. New child node has already
                    // been placed into mChildNodes by caller.
                    // TODO: recycle child node arrays
                    int rightLen = ((newSearchVecEnd - newSearchVecLoc) >> 1) + 2;
                    Node[] rightChildNodes = new Node[rightLen];
                    Node[] leftChildNodes = new Node[mChildNodes.length - rightLen];
                    arraycopy(mChildNodes, leftChildNodes.length, rightChildNodes, 0, rightLen);
                    arraycopy(mChildNodes, 0, leftChildNodes, 0, leftChildNodes.length);
                    newNode.mChildNodes = rightChildNodes;
                    mChildNodes = leftChildNodes;
                }

                newNode.mLeftSegTail = destLoc + encodedLen;
                newNode.mRightSegTail = newPage.length - 1;
                newNode.mSearchVecStart = newSearchVecLoc;
                newNode.mSearchVecEnd = newSearchVecEnd;
                newNode.releaseExclusive();

                // Prune off the right end of this node by shifting vector towards child ids.
                int len = searchVecLoc - searchVecStart;
                arraycopy(page, searchVecStart,
                          page, mSearchVecStart = searchVecEnd + 2 - len, len);
            }

            break;
        } // end doSplit

        mGarbage += garbageAccum;
        mSplit = split;

        result.mKeyLoc = newKeyLoc;

        // Write pointer to key entry.
        encodeShortLE(newPage, newKeyLoc, result.mEntryLoc);

        return result;
    }

    /**
     * Compact internal node by reclaiming garbage and moving search vector
     * towards tail. Caller is responsible for ensuring that new entry will fit
     * after compaction. Space is allocated for new entry, and the search
     * vector points to it.
     *
     * @param encodedLen length of new entry to allocate
     * @param keyPos normalized search vector position of key to insert/update
     * @param childPos normalized search vector position of child node id to insert; pass
     * MIN_VALUE if updating
     */
    private InResult compactInternal(Tree tree, int encodedLen, int keyPos, int childPos) {
        byte[] page = mPage;

        int searchVecLoc = mSearchVecStart;
        keyPos += searchVecLoc;
        // Size of search vector, possibly with new entry.
        int newSearchVecSize = mSearchVecEnd - searchVecLoc + (2 + 2) + (childPos >> 30);

        // Determine new location of search vector, with room to grow on both ends.
        int newSearchVecStart;
        // Capacity available to search vector after compaction.
        int searchVecCap = mGarbage + mRightSegTail + 1 - mLeftSegTail - encodedLen;
        newSearchVecStart = page.length -
            (((searchVecCap + newSearchVecSize + ((newSearchVecSize + 2) << 2)) >> 1) & ~1);

        // Copy into a fresh buffer.

        int destLoc = TN_HEADER_SIZE;
        int newSearchVecLoc = newSearchVecStart;
        int newLoc = 0;
        final int searchVecEnd = mSearchVecEnd;

        Database db = tree.mDatabase;
        byte[] dest = db.removeSpareBuffer();

        for (; searchVecLoc <= searchVecEnd; searchVecLoc += 2, newSearchVecLoc += 2) {
            if (searchVecLoc == keyPos) {
                newLoc = newSearchVecLoc;
                if (childPos >= 0) {
                    newSearchVecLoc += 2;
                } else {
                    continue;
                }
            }
            encodeShortLE(dest, newSearchVecLoc, destLoc);
            int sourceLoc = decodeUnsignedShortLE(page, searchVecLoc);
            int len = keyLengthAtLoc(page, sourceLoc);
            arraycopy(page, sourceLoc, dest, destLoc, len);
            destLoc += len;
        }

        if (childPos >= 0) {
            if (newLoc == 0) {
                newLoc = newSearchVecLoc;
                newSearchVecLoc += 2;
            }

            // Copy child ids, and leave room for inserted child id.
            arraycopy(page, mSearchVecEnd + 2, dest, newSearchVecLoc, childPos);
            arraycopy(page, mSearchVecEnd + 2 + childPos,
                      dest, newSearchVecLoc + childPos + 8,
                      (newSearchVecSize << 2) - childPos);
        } else {
            if (newLoc == 0) {
                newLoc = newSearchVecLoc;
            }

            // Copy child ids.
            arraycopy(page, mSearchVecEnd + 2, dest, newSearchVecLoc, (newSearchVecSize << 2) + 8);
        }

        // Recycle old page buffer.
        db.addSpareBuffer(page);

        // Write pointer to key entry.
        encodeShortLE(dest, newLoc, destLoc);

        mPage = dest;
        mGarbage = 0;
        mLeftSegTail = destLoc + encodedLen;
        mRightSegTail = dest.length - 1;
        mSearchVecStart = newSearchVecStart;
        mSearchVecEnd = newSearchVecLoc - 2;

        InResult result = new InResult();
        result.mPage = dest;
        result.mKeyLoc = newLoc;
        result.mNewChildLoc = newSearchVecLoc + childPos;
        result.mEntryLoc = destLoc;

        return result;
    }

    /**
     * Provides information necessary to complete split by copying split key, pointer to
     * split key, and pointer to new child id.
     */
    static final class InResult {
        byte[] mPage;
        int mKeyLoc;      // location of key reference in search vector
        int mNewChildLoc; // location of child pointer
        int mEntryLoc;    // location of key entry, referenced by search vector
    }

    private Split newSplitLeft(Node newNode) {
        Split split = new Split(false, newNode);
        // New left node cannot be a high extremity, and this node cannot be a low extremity.
        newNode.mType = (byte) (mType & ~HIGH_EXTREMITY);
        mType &= ~LOW_EXTREMITY;
        return split;
    }

    private Split newSplitRight(Node newNode) {
        Split split = new Split(true, newNode);
        // New right node cannot be a low extremity, and this node cannot be a high extremity.
        newNode.mType = (byte) (mType & ~LOW_EXTREMITY);
        mType &= ~HIGH_EXTREMITY;
        return split;
    }
}
