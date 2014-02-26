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
 * Mapping of keys to values, in no particular order. Subclasses and
 * implementations may specify an explicit ordering.
 *
 * @author Brian S O'Neill
 * @see Database
 */
public interface View {
    /**
     * Returns a copy of the value for the given key, or null if no matching
     * entry exists.
     *
     * <p>If the entry must be locked, ownership of the key instance is
     * transferred. The key must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for {@link
     * LockMode#READ_COMMITTED READ_COMMITTED} locking behavior
     * @param key non-null key
     * @return copy of value, or null if entry doesn't exist
     * @throws NullPointerException if key is null
     * @throws IllegalArgumentException if transaction belongs to another database instance
     */
    public byte[] load(byte[] key) throws IOException;
}
