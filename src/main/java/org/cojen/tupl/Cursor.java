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

/**
 * Maintains a logical position in a {@link View}. Cursor instances can only be
 * safely used by one thread at a time, and they must be {@link #reset reset}
 * when no longer needed. Instances can be exchanged by threads, as long as a
 * happens-before relationship is established. Without proper exclusion,
 * multiple threads interacting with a Cursor instance may cause database
 * corruption.
 *
 * @author Brian S O'Neill
 * @see View#newCursor View.newCursor
 */
public interface Cursor {
    /**
     * Empty marker which indicates that value exists but has not been {@link
     * #load loaded}.
     */
    public static final byte[] NOT_LOADED = new byte[0];

    /**
     * Returns an uncopied reference to the current key, or null if Cursor is
     * unpositioned. Array contents must not be modified.
     */
    public byte[] key();

    /**
     * Returns an uncopied reference to the current value, which might be null
     * or {@link #NOT_LOADED}. Array contents can be safely modified.
     */
    public byte[] value();

    /**
     * Moves the Cursor to find the given key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it must
     * not be modified after calling this method.
     * @throws NullPointerException if key is null
     */
    public void find(byte[] key) throws IOException;

    /**
     * Stores a value into the current entry, leaving the position
     * unchanged. An entry may be inserted, updated or deleted by this
     * method. A null value deletes the entry. Unless an exception is thrown,
     * the object returned by the {@link #value value} method will be the same
     * instance as was provided to this method.
     *
     * @param value value to store; pass null to delete
     * @throws IllegalStateException if position is undefined at invocation time
     * @throws ViewConstraintException if value is not permitted
     */
    public void store(byte[] value) throws IOException;

    /**
     * Resets Cursor and moves it to an undefined position. The key and value references are
     * also cleared.
     */
    public void reset();
}
