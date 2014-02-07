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

import java.util.Arrays;

import static java.lang.System.arraycopy;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class Utils {
    static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * Encodes a 16-bit integer, in little-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static final void encodeShortLE(byte[] b, int offset, int v) {
        b[offset    ] = (byte)v;
        b[offset + 1] = (byte)(v >> 8);
    }

    /**
     * Encodes a 32-bit integer, in little-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static final void encodeIntLE(byte[] b, int offset, int v) {
        b[offset    ] = (byte)v;
        b[offset + 1] = (byte)(v >> 8);
        b[offset + 2] = (byte)(v >> 16);
        b[offset + 3] = (byte)(v >> 24);
    }

    /**
     * Encodes a 64-bit integer, in little-endian format.
     *
     * @param b encode destination
     * @param offset offset into byte array
     * @param v value to encode
     */
    public static final void encodeLongLE(byte[] b, int offset, long v) {
        int w = (int)v;
        b[offset    ] = (byte)w;
        b[offset + 1] = (byte)(w >> 8);
        b[offset + 2] = (byte)(w >> 16);
        b[offset + 3] = (byte)(w >> 24);
        w = (int)(v >> 32);
        b[offset + 4] = (byte)w;
        b[offset + 5] = (byte)(w >> 8);
        b[offset + 6] = (byte)(w >> 16);
        b[offset + 7] = (byte)(w >> 24);
    }

    /**
     * Decodes a 16-bit unsigned integer, in little-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static final int decodeUnsignedShortLE(byte[] b, int offset) {
        return ((b[offset] & 0xff)) | ((b[offset + 1] & 0xff) << 8);
    }

    /**
     * Decodes a 32-bit integer, in little-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static final int decodeIntLE(byte[] b, int offset) {
        return (b[offset] & 0xff) | ((b[offset + 1] & 0xff) << 8) |
            ((b[offset + 2] & 0xff) << 16) | (b[offset + 3] << 24);
    }

    /**
     * Decodes a 64-bit integer, in little-endian format.
     *
     * @param b decode source
     * @param offset offset into byte array
     * @return decoded value
     */
    public static final long decodeLongLE(byte[] b, int offset) {
        return
            (((long)(((b[offset    ] & 0xff)      ) |
                     ((b[offset + 1] & 0xff) << 8 ) |
                     ((b[offset + 2] & 0xff) << 16) |
                     ((b[offset + 3]       ) << 24)) & 0xffffffffL)      ) |
            (((long)(((b[offset + 4] & 0xff)      ) |
                     ((b[offset + 5] & 0xff) << 8 ) |
                     ((b[offset + 6] & 0xff) << 16) |
                     ((b[offset + 7]       ) << 24))              ) << 32);
    }

    /**
     * Performs multiple array copies, correctly ordered to prevent clobbering. The copies
     * must not overlap, and start1 must be less than start2.
     */
    static void arrayCopies(byte[] page,
                            int start1, int dest1, int length1,
                            int start2, int dest2, int length2)
    {
        if (dest1 < start1) {
            arraycopy(page, start1, page, dest1, length1);
            arraycopy(page, start2, page, dest2, length2);
        } else {
            arraycopy(page, start2, page, dest2, length2);
            arraycopy(page, start1, page, dest1, length1);
        }
    }

    /**
     * Performs multiple array copies, correctly ordered to prevent clobbering. The copies
     * must not overlap, start1 must be less than start2, and start2 be less than start3.
     */
    static void arrayCopies(byte[] page,
                            int start1, int dest1, int length1,
                            int start2, int dest2, int length2,
                            int start3, int dest3, int length3)
    {
        if (dest1 < start1) {
            arraycopy(page, start1, page, dest1, length1);
            arrayCopies(page, start2, dest2, length2, start3, dest3, length3);
        } else {
            arrayCopies(page, start2, dest2, length2, start3, dest3, length3);
            arraycopy(page, start1, page, dest1, length1);
        }
    }

    /**
     * @return negative if 'a' is less, zero if equal, greater than zero if greater
     */
    static int compareKeys(byte[] a, byte[] b) {
        return compareKeys(a, 0, a.length, b, 0, b.length);
    }

    /**
     * @param a key 'a'
     * @param aoff key 'a' offset
     * @param alen key 'a' length
     * @param b key 'b'
     * @param boff key 'b' offset
     * @param blen key 'b' length
     * @return negative if 'a' is less, zero if equal, greater than zero if greater
     */
    static int compareKeys(byte[] a, int aoff, int alen, byte[] b, int boff, int blen) {
        int minLen = Math.min(alen, blen);
        for (int i=0; i<minLen; i++) {
            byte ab = a[aoff + i];
            byte bb = b[boff + i];
            if (ab != bb) {
                return (ab & 0xff) - (bb & 0xff);
            }
        }
        return alen - blen;
    }

    /**
     * Returns a new key, midway between the given low and high keys. Returned key is never
     * equal to the low key, but it might be equal to the high key. If high key is not actually
     * higher than the given low key, an ArrayIndexOfBoundException might be thrown.
     *
     * <p>Method is used for internal node suffix compression. To disable, simply return a copy
     * of the high key.
     */
    static byte[] midKey(byte[] low, int lowOff, int lowLen,
                         byte[] high, int highOff, int highLen)
    {
        byte[] mid = new byte[highLen];
        System.arraycopy(high, highOff, mid, 0, mid.length);
        return mid;
    }
}
