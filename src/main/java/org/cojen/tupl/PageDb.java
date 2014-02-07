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
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Brian S O'Neill
 * @see DurablePageDb
 * @see NonPageDb
 */
abstract class PageDb {
    final ReentrantReadWriteLock mCommitLock;

    PageDb() {
        // Need to use a reentrant lock instead of a latch to simplify the
        // logic for persisting in-flight undo logs during a checkpoint. Pages
        // might need to be allocated during this time, and so reentrancy is
        // required to avoid deadlock. Ideally, lock should be fair in order
        // for exclusive lock request to de-prioritize itself by timing out and
        // retrying. See Database.checkpoint. Being fair slows down overall
        // performance, because it increases the cost of acquiring the shared
        // lock. For this reason, it isn't fair.
        mCommitLock = new ReentrantReadWriteLock(false);
    }

    /**
     * Returns the fixed size of all pages in the store, in bytes.
     */
    public abstract int pageSize();

    /**
     * Allocates a page to be written to.
     *
     * @return page id; never zero or one
     */
    public abstract long allocPage() throws IOException;

    /**
     * Access the shared commit lock, which prevents commits while held.
     */
    public Lock sharedCommitLock() {
        return mCommitLock.readLock();
    }

    /**
     * Access the exclusive commit lock, which is acquired by the commit method.
     */
    public Lock exclusiveCommitLock() {
        return mCommitLock.writeLock();
    }
}
