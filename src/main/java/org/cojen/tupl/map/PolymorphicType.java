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

package org.cojen.tupl.map;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class PolymorphicType extends Type {
    private static final long HASH_BASE = 1902668584782181472L;

    PolymorphicType(Schemata schemata, long typeId, short flags) {
        super(schemata, typeId, flags);
    }

    public long getIdentifier() {
        // FIXME
        throw null;
    }

    public Type getTargetType() {
        // FIXME
        throw null;
    }

    static long computeHash(short flags, Type nameType, byte[] name) {
        long hash = mixHash(HASH_BASE + flags, nameType);
        hash = mixHash(hash, name);
        return hash;
    }

    static byte[] encodeValue(short flags, Type nameType, byte[] name, Type[] allowedTypes) {
        // FIXME: polymorph prefix 6, uint_16 flags, MapType<uvarint_64, uvarint_64>
        throw null;
    }
}
