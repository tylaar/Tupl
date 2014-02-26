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
}
