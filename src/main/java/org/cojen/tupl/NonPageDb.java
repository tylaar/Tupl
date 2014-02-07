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

import java.util.Arrays;
import java.util.BitSet;

/**
 * PageDb implementation which doesn't actually work. Used for non-durable
 * databases.
 *
 * @author Brian S O'Neill
 */
class NonPageDb extends PageDb {
    private final int mPageSize;

    NonPageDb(int pageSize) {
        mPageSize = pageSize;
    }

    @Override
    public int pageSize() {
        return mPageSize;
    }

    @Override
    public long allocPage() throws IOException {
        // For ordinary nodes, the same identifier can be vended out each
        // time. Fragmented values require unique identifiers.
        return 2;
    }
}
