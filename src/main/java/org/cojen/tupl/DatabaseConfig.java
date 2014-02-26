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

/**
 * Configuration options used when {@link Database#open opening} a database.
 *
 * @author Brian S O'Neill
 */
public class DatabaseConfig {
    long mMinCachedBytes;
    long mMaxCachedBytes;
    int mPageSize;

    public DatabaseConfig() {
    }

    /**
     * Set the minimum cache size, overriding the default.
     *
     * @param minBytes cache size, in bytes
     */
    public DatabaseConfig minCacheSize(long minBytes) {
        mMinCachedBytes = minBytes;
        return this;
    }

    /**
     * Set the maximum cache size, overriding the default.
     *
     * @param maxBytes cache size, in bytes
     */
    public DatabaseConfig maxCacheSize(long maxBytes) {
        mMaxCachedBytes = maxBytes;
        return this;
    }

    /**
     * Set the page size, which is 4096 bytes by default.
     */
    public DatabaseConfig pageSize(int size) {
        mPageSize = size;
        return this;
    }
}
