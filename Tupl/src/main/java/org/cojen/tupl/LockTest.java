/*
 *  Copyright 2011 Brian S O'Neill
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

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.LockResult.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class LockTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LockTest.class.getName());
    }

    private static final byte[] k1, k2, k3, k4;

    private static final long ONE_MILLIS_IN_NANOS = 1000000L;

    private static final long SHORT_TIMEOUT = ONE_MILLIS_IN_NANOS;
    private static final long MEDIUM_TIMEOUT = ONE_MILLIS_IN_NANOS * 10000;

    static {
        k1 = key("hello");
        k2 = key("world");
        k3 = key("akey");
        k4 = key("bkey");
    }

    public static byte[] key(String str) {
        return str.getBytes();
    }

    public static void sleep(long millis) {
        if (millis == 0) {
            return;
        }
        long start = System.nanoTime();
        do {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
            }
            millis -= (System.nanoTime() - start) / 1000000;
        } while (millis > 0);
    }

    public static void selfInterrupt(final long delayMillis) {
        final Thread thread = Thread.currentThread();
        new Thread() {
            public void run() {
                LockTest.sleep(delayMillis);
                thread.interrupt();
            }
        }.start();
    }

    private LockManager mManager;
    private ExecutorService mExecutor;

    @Before
    public void setup() {
        mManager = new LockManager();
    }

    @After
    public void teardown() {
        if (mExecutor != null) {
            mExecutor.shutdown();
        }
    }

    @Test
    public void basicShared() {
        try {
            Locker locker = new Locker(null);
            fail();
        } catch (IllegalArgumentException e) {
        }

        Locker locker = new Locker(mManager);
        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(ACQUIRED, locker.tryLockShared(0, k2, -1));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, -1));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k2, -1));
        assertEquals(2, mManager.numLocksHeld());
        locker.reset();
        assertEquals(0, mManager.numLocksHeld());

        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockShared(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.unlock();
        assertEquals(k1, locker.lastLockedKey());
        locker.unlock();
        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockShared(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.unlock();

        Locker locker2 = new Locker(mManager);
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k2, -1));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockShared(0, k2, -1));

        assertEquals(k2, locker.lastLockedKey());
        locker.unlockToShared();
        assertEquals(k2, locker2.lastLockedKey());
        locker2.unlockToShared();
        assertEquals(k2, locker2.lastLockedKey());
        locker2.unlockToShared();

        assertEquals(OWNED_SHARED, locker2.tryLockShared(0, k1, -1));
        assertEquals(OWNED_SHARED, locker2.tryLockShared(0, k2, -1));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, -1));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k2, -1));

        locker.reset();
        locker2.reset();

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, -1));

        assertEquals(ILLEGAL, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ILLEGAL, locker.tryLockExclusive(0, k1, -1));

        locker.reset();
        locker2.reset();
    }

    @Test
    public void basicUpgradable() {
        Locker locker = new Locker(mManager);
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(OWNED_UPGRADABLE, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(2, mManager.numLocksHeld());
        locker.reset();
        assertEquals(0, mManager.numLocksHeld());

        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.unlock();
        assertEquals(k1, locker.lastLockedKey());
        locker.unlock();
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.unlock();

        Locker locker2 = new Locker(mManager);
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockUpgradable(0, k1, SHORT_TIMEOUT));
        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k2, -1));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k2, SHORT_TIMEOUT));
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, -1));
        assertEquals(OWNED_SHARED, locker2.tryLockShared(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker2.tryLockShared(0, k2, -1));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockShared(0, k2, -1));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k2, -1));

        try {
            locker.unlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }
        assertEquals(k2, locker.lastLockedKey());
        locker.unlockToShared();
        try {
            locker2.unlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }
        assertEquals(k1, locker2.lastLockedKey());
        locker2.unlockToShared();
        assertEquals(k1, locker2.lastLockedKey());
        locker2.unlockToShared();

        assertEquals(ILLEGAL, locker2.tryLockUpgradable(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker2.tryLockUpgradable(0, k2, -1));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ILLEGAL, locker.tryLockUpgradable(0, k2, -1));

        locker.reset();
        locker2.reset();

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));

        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToUpgradable();
        assertEquals(OWNED_UPGRADABLE, locker.tryLockShared(0, k1, -1));
        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToShared();
        
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, -1));
        assertEquals(ILLEGAL, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ILLEGAL, locker.tryLockExclusive(0, k1, -1));

        locker.reset();
        locker2.reset();
    }

    @Test
    public void basicExclusive() {
        Locker locker = new Locker(mManager);
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, k1, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, k2, -1));
        assertEquals(2, mManager.numLocksHeld());
        locker.reset();
        assertEquals(0, mManager.numLocksHeld());

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.unlock();
        assertEquals(k1, locker.lastLockedKey());
        locker.unlock();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.unlock();

        Locker locker2 = new Locker(mManager);
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockShared(0, k1, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockUpgradable(0, k1, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockExclusive(0, k1, SHORT_TIMEOUT));
        assertEquals(ACQUIRED, locker2.tryLockExclusive(0, k2, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockShared(0, k2, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k2, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockExclusive(0, k2, SHORT_TIMEOUT));

        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToUpgradable();
        assertEquals(OWNED_UPGRADABLE, locker.tryLockShared(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToShared();
        assertEquals(k2, locker2.lastLockedKey());
        locker2.unlockToUpgradable();
        assertEquals(k2, locker2.lastLockedKey());
        locker2.unlockToShared();
        assertEquals(OWNED_SHARED, locker2.tryLockShared(0, k2, -1));

        assertEquals(ILLEGAL, locker.tryLockExclusive(0, k1, -1));
        assertEquals(ILLEGAL, locker2.tryLockExclusive(0, k2, -1));

        locker.reset();
        locker2.reset();
        locker2.reset();

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));

        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToUpgradable();
        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToUpgradable();
        assertEquals(OWNED_UPGRADABLE, locker.tryLockShared(0, k1, -1));
        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToShared();
        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToShared();
        
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, -1));
        assertEquals(ILLEGAL, locker.tryLockExclusive(0, k1, -1));

        locker.reset();
        locker2.reset();
    }

    @Test
    public void isolatedIndexes() {
        Locker locker = new Locker(mManager);
        assertEquals(ACQUIRED, locker.tryLockExclusive(1, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(2, k1, -1));
        assertEquals(2, mManager.numLocksHeld());
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(1, k1, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(2, k1, -1));
        assertEquals(2, mManager.numLocksHeld());
        assertEquals(2, locker.lastLockedIndex());
        assertEquals(k1, locker.lastLockedKey());
        locker.reset();
        assertEquals(0, mManager.numLocksHeld());
    }

    @Test
    public void upgrade() {
        Locker locker = new Locker(mManager);
        Locker locker2 = new Locker(mManager);

        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockExclusive(0, k1, SHORT_TIMEOUT));
        scheduleUnlock(locker, 1000);
        assertEquals(UPGRADED, locker2.tryLockExclusive(0, k1, MEDIUM_TIMEOUT));
        assertEquals(OWNED_EXCLUSIVE, locker2.tryLockExclusive(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockShared(0, k1, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k1, SHORT_TIMEOUT));
        scheduleUnlockToUpgradable(locker2, 1000);
        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, MEDIUM_TIMEOUT));
        assertEquals(OWNED_UPGRADABLE, locker2.tryLockUpgradable(0, k1, -1));

        locker.reset();
        locker2.reset();
    }

    @Test
    public void downgrade() {
        Locker locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.tryLockExclusive(0, k1, -1));
        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
            // Non-immediate upgrade.
        }
        try {
            locker.unlockToShared();
            fail();
        } catch (IllegalStateException e) {
            // Non-immediate upgrade.
        }
        assertArrayEquals(k1, locker.lastLockedKey());
        locker.unlockToUpgradable();
        assertArrayEquals(k2, locker.lastLockedKey());
        locker.unlock();
        assertArrayEquals(k1, locker.lastLockedKey());
        locker.unlock();

        // Do again, but with another upgrade in between.

        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.tryLockExclusive(0, k2, -1));
        assertEquals(UPGRADED, locker.tryLockExclusive(0, k1, -1));
        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
            // Non-immediate upgrade.
        }
        try {
            locker.unlockToShared();
            fail();
        } catch (IllegalStateException e) {
            // Non-immediate upgrade.
        }
        assertArrayEquals(k1, locker.lastLockedKey());
        locker.unlockToUpgradable();
        assertArrayEquals(k2, locker.lastLockedKey());
        locker.unlock();
        assertArrayEquals(k1, locker.lastLockedKey());
        locker.unlock();

        try {
            locker.unlockToShared();
            fail();
        } catch (IllegalStateException e) {
            // No locks held.
        }
        try {
            locker.unlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
            // No locks held.
        }
    }

    @Test
    public void pileOfLocks() {
        Locker locker = new Locker(mManager);
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        assertEquals(1000, mManager.numLocksHeld());
        locker.reset();
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        for (int i=0; i<1000; i++) {
            assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        for (int i=1000; --i>=0; ) {
            assertArrayEquals(key("k" + i), locker.lastLockedKey());
            locker.unlock();
        }
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        locker.reset();
    }

    @Test
    public void blockedNoWait() {
        blocked(0);
    }

    @Test
    public void blockedTimedWait() {
        blocked(SHORT_TIMEOUT);
    }

    private void blocked(long nanosTimeout) {
        Locker locker = new Locker(mManager);
        Locker locker2 = new Locker(mManager);

        locker.tryLockShared(0, k1, -1);

        assertEquals(TIMED_OUT_LOCK, locker2.tryLockExclusive(0, k1, nanosTimeout));

        locker.unlock();

        assertEquals(ACQUIRED, locker2.tryLockExclusive(0, k1, -1));
        locker2.unlock();

        locker.tryLockUpgradable(0, k1, -1);

        assertEquals(TIMED_OUT_LOCK, locker2.tryLockUpgradable(0, k1, nanosTimeout));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockExclusive(0, k1, nanosTimeout));

        locker.unlock();

        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, -1));
        locker2.unlock();

        locker.tryLockExclusive(0, k1, -1);

        assertEquals(TIMED_OUT_LOCK, locker2.tryLockShared(0, k1, nanosTimeout));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockUpgradable(0, k1, nanosTimeout));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockExclusive(0, k1, nanosTimeout));

        locker.unlock();

        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, -1));
        locker2.unlock();

        locker.reset();
        locker2.reset();
    }

    @Test
    public void interrupts() {
        interrupts(-1);
    }

    @Test
    public void interruptsTimedWait() {
        interrupts(10000 * ONE_MILLIS_IN_NANOS);
    }

    private void interrupts(long nanosTimeout) {
        Locker locker = new Locker(mManager);
        Locker locker2 = new Locker(mManager);

        locker.tryLockShared(0, k1, -1);

        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.tryLockExclusive(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());

        locker.unlock();

        assertEquals(ACQUIRED, locker2.tryLockExclusive(0, k1, -1));
        locker2.unlock();

        locker.tryLockUpgradable(0, k1, -1);

        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.tryLockUpgradable(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());
        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.tryLockExclusive(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());

        locker.unlock();

        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, -1));
        locker2.unlock();

        locker.tryLockExclusive(0, k1, -1);

        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.tryLockShared(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());
        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.tryLockUpgradable(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());
        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.tryLockExclusive(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());

        locker.unlock();

        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, -1));
        locker2.unlock();

        locker.reset();
        locker2.reset();
    }

    @Test
    public void delayedAcquire() {
        Locker locker = new Locker(mManager);
        Locker locker2 = new Locker(mManager);
        long end;

        // Exclusive locks blocked...

        // Exclusive lock blocked by shared lock.
        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockExclusive(0, k1, MEDIUM_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockShared(0, k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Exclusive lock blocked by upgradable lock.
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockExclusive(0, k1, MEDIUM_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Exclusive lock blocked by exclusive lock.
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockExclusive(0, k1, MEDIUM_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Upgradable locks blocked...

        // Upgradable lock blocked by upgradable lock.
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, MEDIUM_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Upgradable lock blocked by upgradable lock, granted via downgrade to shared.
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        end = scheduleUnlockToShared(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, MEDIUM_TIMEOUT));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, 0));
        locker2.unlock();
        locker.unlock();
        assertTrue(System.nanoTime() >= end);

        // Upgradable lock blocked by exclusive lock.
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, MEDIUM_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Upgradable lock blocked by exclusive lock, granted via downgrade to shared.
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        end = scheduleUnlockToShared(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, MEDIUM_TIMEOUT));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, 0));
        locker2.unlock();
        locker.unlock();
        assertTrue(System.nanoTime() >= end);

        // Shared locks blocked...

        // Shared lock blocked by exclusive lock.
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, MEDIUM_TIMEOUT));
        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, 0));
        locker.unlock();
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Shared lock blocked by exclusive lock, granted via downgrade to shared.
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        end = scheduleUnlockToShared(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, MEDIUM_TIMEOUT));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, 0));
        locker.unlock();
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Shared lock blocked by exclusive lock, granted via downgrade to upgradable.
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        end = scheduleUnlockToUpgradable(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, MEDIUM_TIMEOUT));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockShared(0, k1, 0));
        locker.unlock();
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);
    }

    @Test
    public void fifo() throws Exception {
        mExecutor = Executors.newCachedThreadPool();

        final int count = 10;

        Locker[] lockers = new Locker[count];
        for (int i=0; i<count; i++) {
            lockers[i] = new Locker(mManager);
        }
        Future<LockResult>[] futures = new Future[count];

        // Upgradable locks acquired in fifo order.
        synchronized (lockers[0]) {
            lockers[0].tryLockUpgradable(0, k1, -1);
        }
        for (int i=1; i<lockers.length; i++) {
            // Sleep between attempts, to ensure proper enqueue ordering while
            // threads are concurrently starting.
            sleep(100);
            futures[i] = tryLockUpgradable(lockers[i], 0, k1);
        }
        for (int i=1; i<lockers.length; i++) {
            Locker last = lockers[i - 1];
            synchronized (last) {
                last.unlock();
            }
            assertEquals(ACQUIRED, futures[i].get());
        }

        // Clean up.
        for (int i=0; i<count; i++) {
            synchronized (lockers[i]) {
                lockers[i].reset();
            }
        }

        // Shared lock, enqueue exclusive lock, remaining shared locks must wait.
        synchronized (lockers[0]) {
            lockers[0].tryLockShared(0, k1, -1);
        }
        synchronized (lockers[1]) {
            assertEquals(TIMED_OUT_LOCK, lockers[1].tryLockExclusive(0, k1, SHORT_TIMEOUT));
        }
        futures[1] = tryLockExclusive(lockers[1], 0, k1);
        sleep(100);
        synchronized (lockers[2]) {
            assertEquals(TIMED_OUT_LOCK, lockers[2].tryLockShared(0, k1, SHORT_TIMEOUT));
        }
        for (int i=2; i<lockers.length; i++) {
            sleep(100);
            futures[i] = tryLockShared(lockers[i], 0, k1);
        }
        // Now release first shared lock.
        synchronized (lockers[0]) {
            lockers[0].unlock();
        }
        // Exclusive lock is now available.
        assertEquals(ACQUIRED, futures[1].get());
        // Verify shared locks not held.
        sleep(100);
        for (int i=2; i<lockers.length; i++) {
            assertFalse(futures[i].isDone());
        }
        // Release exclusive and let shared in.
        synchronized (lockers[1]) {
            lockers[1].unlock();
        }
        // Shared locks all acquired now.
        for (int i=2; i<lockers.length; i++) {
            assertEquals(ACQUIRED, futures[i].get());
        }

        // Clean up.
        for (int i=0; i<count; i++) {
            synchronized (lockers[i]) {
                lockers[i].reset();
            }
        }
    }

    @Test
    public void scoping() {
        // Lots o' sub tests, many of which were created to improve code coverage.

        Locker locker = new Locker(mManager);

        assertFalse(locker.scopeExit(false));
        assertFalse(locker.scopeExit(true));

        locker.scopeEnter();
        assertTrue(locker.scopeExit(false));
        assertFalse(locker.scopeExit(true));

        locker.scopeEnter();
        assertTrue(locker.scopeExit(true));
        assertFalse(locker.scopeExit(false));

        locker.scopeEnter();
        locker.scopeEnter();
        assertTrue(locker.scopeExit(false));
        assertTrue(locker.scopeExit(true));
        assertFalse(locker.scopeExit(false));

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, k1, -1));
        assertTrue(locker.scopeExit(false));
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, k1, -1));
        assertFalse(locker.scopeExit(false));
        assertEquals(UNOWNED, locker.check(0, k1));

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        assertTrue(locker.scopeExit(false));
        assertEquals(UNOWNED, locker.check(0, k2));
        assertEquals(OWNED_EXCLUSIVE, locker.check(0, k1));
        locker.reset();

        // Upgrade lock within scope, and then exit scope.
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        locker.scopeEnter();
        assertEquals(UPGRADED, locker.tryLockExclusive(0, k1, -1));
        assertTrue(locker.scopeExit(false));
        // Outer scope was downgraded to original lock strength.
        assertEquals(OWNED_UPGRADABLE, locker.check(0, k1));
        locker.reset();

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k3, -1));
        // Reset and unlock all scopes.
        locker.reset();
        assertEquals(UNOWNED, locker.check(0, k1));
        assertEquals(UNOWNED, locker.check(0, k2));
        assertEquals(UNOWNED, locker.check(0, k3));
        assertFalse(locker.scopeExit(false));

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k3, -1));
        // Reset and unlock all scopes.
        locker.reset();
        assertEquals(UNOWNED, locker.check(0, k1));
        assertEquals(UNOWNED, locker.check(0, k2));
        assertEquals(UNOWNED, locker.check(0, k3));
        assertFalse(locker.scopeExit(false));

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k3, -1));
        assertTrue(locker.scopeExit(true));
        assertEquals(OWNED_EXCLUSIVE, locker.check(0, k3));
        assertTrue(locker.scopeExit(true));
        assertEquals(OWNED_EXCLUSIVE, locker.check(0, k2));
        assertFalse(locker.scopeExit(true));
        assertEquals(UNOWNED, locker.check(0, k1));
        assertEquals(UNOWNED, locker.check(0, k2));
        assertEquals(UNOWNED, locker.check(0, k3));
        
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k3, -1));
        assertTrue(locker.scopeExit(true));
        assertEquals(OWNED_EXCLUSIVE, locker.check(0, k3));
        assertEquals(OWNED_EXCLUSIVE, locker.check(0, k2));
        assertFalse(locker.scopeExit(true));
        assertEquals(UNOWNED, locker.check(0, k1));
        assertEquals(UNOWNED, locker.check(0, k2));
        assertEquals(UNOWNED, locker.check(0, k3));

        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k3, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k4, -1));
        assertTrue(locker.scopeExit(true));
        assertEquals(OWNED_EXCLUSIVE, locker.check(0, k3));
        assertEquals(OWNED_EXCLUSIVE, locker.check(0, k4));
        assertTrue(locker.scopeExit(true));
        assertFalse(locker.scopeExit(true));
        assertEquals(UNOWNED, locker.check(0, k1));
        assertEquals(UNOWNED, locker.check(0, k2));
        assertEquals(UNOWNED, locker.check(0, k3));
        assertEquals(UNOWNED, locker.check(0, k4));

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        // Fill up first block of locks.
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        assertTrue(locker.scopeExit(true));
        assertEquals(OWNED_EXCLUSIVE, locker.check(0, k1));
        for (int i=0; i<8; i++) {
            assertEquals(OWNED_EXCLUSIVE, locker.check(0, key("k" + i)));
        }
        assertFalse(locker.scopeExit(true));
        assertEquals(UNOWNED, locker.check(0, k1));
        for (int i=0; i<8; i++) {
            assertEquals(UNOWNED, locker.check(0, key("k" + i)));
        }

        // Fill up first block of locks.
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        locker.scopeEnter();
        // Fill up another first block of locks.
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("a" + i), -1));
        }
        assertTrue(locker.scopeExit(true));
        for (int i=0; i<8; i++) {
            assertEquals(OWNED_EXCLUSIVE, locker.check(0, key("k" + i)));
            assertEquals(OWNED_EXCLUSIVE, locker.check(0, key("a" + i)));
        }
        assertFalse(locker.scopeExit(true));
        for (int i=0; i<8; i++) {
            assertEquals(UNOWNED, locker.check(0, key("k" + i)));
            assertEquals(UNOWNED, locker.check(0, key("a" + i)));
        }

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        locker.scopeEnter();
        // Fill up first block of locks.
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        for (int q=0; q<2; q++) {
            assertTrue(locker.scopeExit(true));
            assertEquals(OWNED_EXCLUSIVE, locker.check(0, k1));
            assertEquals(OWNED_EXCLUSIVE, locker.check(0, k2));
            for (int i=0; i<8; i++) {
                assertEquals(OWNED_EXCLUSIVE, locker.check(0, key("k" + i)));
            }
        }
        assertFalse(locker.scopeExit(true));
        assertEquals(UNOWNED, locker.check(0, k1));
        assertEquals(UNOWNED, locker.check(0, k2));
        for (int i=0; i<8; i++) {
            assertEquals(UNOWNED, locker.check(0, key("k" + i)));
        }

        // Deep scoping.
        for (int i=0; i<10; i++) {
            locker.scopeEnter();
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        for (int i=10; --i>=0; ) {
            locker.scopeExit(false);
            assertEquals(UNOWNED, locker.check(0, key("k" + i)));
        }
        locker.reset();

        for (int q=0; q<3; q++) {
            for (int w=0; w<2; w++) {
                if (w != 0) {
                    for (int i=0; i<100; i++) {
                        assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("v" + i), -1));
                    }
                }

                assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
                locker.scopeEnter();
                // Fill up first block of locks.
                for (int i=0; i<8; i++) {
                    assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
                }
                assertTrue(locker.scopeExit(true));
                for (int i=0; i<8; i++) {
                    locker.unlock();
                }
                assertEquals(UPGRADED, locker.tryLockExclusive(0, k1, -1));
                if (q == 0) {
                    locker.unlockToShared();
                    assertEquals(OWNED_SHARED, locker.check(0, k1));
                } else if (q == 1) {
                    locker.unlockToUpgradable();
                    assertEquals(OWNED_UPGRADABLE, locker.check(0, k1));
                } else {
                    locker.unlock();
                    assertEquals(UNOWNED, locker.check(0, k1));
                }
                assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
                locker.reset();
                assertEquals(UNOWNED, locker.check(0, k1));
                for (int i=0; i<8; i++) {
                    assertEquals(UNOWNED, locker.check(0, key("k" + i)));
                }
            }
        }

        // Create a chain with alternating tiny blocks.
        for (int q=0; q<4; q++) {
            locker.scopeEnter();
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("q" + q), -1));
            locker.scopeEnter();
            for (int i=0; i<8; i++) {
                assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + q + "_" + i), -1));
            }
        }
        for (int q=0; q<8; q++) {
            locker.scopeExit(true);
        }
        for (int q=4; --q>=0; ) {
            for (int i=8; --i>=0; ) {
                assertArrayEquals(key("k" + q + "_" + i), locker.lastLockedKey());
                locker.unlock();
            }
            assertArrayEquals(key("q" + q), locker.lastLockedKey());
            locker.unlock();
        }

        for (int q=0; q<2; q++) {
            assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
            locker.scopeEnter();
            if (q != 0) {
                for (int i=0; i<(8 + 16); i++) {
                    assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("v" + i), -1));
                }
            }
            assertEquals(UPGRADED, locker.tryLockExclusive(0, k1, -1));
            assertEquals(ACQUIRED, locker.tryLockShared(0, k2, -1));
            locker.unlock();
            assertEquals(UNOWNED, locker.check(0, k2));
            assertEquals(OWNED_EXCLUSIVE, locker.check(0, k1));
            locker.unlockToUpgradable();
            assertEquals(OWNED_UPGRADABLE, locker.check(0, k1));
            locker.reset();
            assertEquals(UNOWNED, locker.check(0, k1));
        }

        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        assertTrue(locker.scopeExit(true));
        for (int i=8; --i>=0; ) {
            locker.unlock();
            assertEquals(UNOWNED, locker.check(0, key("k" + i)));
        }
        locker.scopeEnter();
        for (int i=0; i<4; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("v" + i), -1));
        }
        assertTrue(locker.scopeExit(true));
        assertEquals(OWNED_EXCLUSIVE, locker.check(0, k1));
        for (int i=0; i<4; i++) {
            assertEquals(OWNED_EXCLUSIVE, locker.check(0, key("v" + i)));
        }
        for (int i=4; --i>=0; ) {
            locker.unlock();
            assertEquals(UNOWNED, locker.check(0, key("v" + i)));
        }
        locker.unlock();
        assertEquals(UNOWNED, locker.check(0, k1));
        for (int i=0; i<8; i++) {
            assertEquals(UNOWNED, locker.check(0, key("k" + i)));
        }
        assertTrue(locker.scopeExit(false));
        assertFalse(locker.scopeExit(false));

        for (int i=0; i<9; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        locker.scopeEnter();
        for (int i=0; i<4; i++) {
            assertEquals(ACQUIRED, locker.tryLockUpgradable(0, key("v" + i), -1));
        }
        locker.scopeExit(true);
        for (int i=0; i<9; i++) {
            assertEquals(OWNED_EXCLUSIVE, locker.check(0, key("k" + i)));
        }
        for (int i=0; i<4; i++) {
            assertEquals(OWNED_UPGRADABLE, locker.check(0, key("v" + i)));
        }
        assertEquals(false, locker.scopeExit(true));
        for (int i=0; i<9; i++) {
            assertEquals(UNOWNED, locker.check(0, key("k" + i)));
        }
        for (int i=0; i<4; i++) {
            assertEquals(UNOWNED, locker.check(0, key("v" + i)));
        }

    }

    private long scheduleUnlock(final Locker locker, final long delayMillis) {
        return schedule(locker, delayMillis, 0);
    }

    private long scheduleUnlockToShared(final Locker locker, final long delayMillis) {
        return schedule(locker, delayMillis, 1);
    }

    private long scheduleUnlockToUpgradable(final Locker locker, final long delayMillis) {
        return schedule(locker, delayMillis, 2);
    }

    private long schedule(final Locker locker, final long delayMillis, final int type) {
        long end = System.nanoTime() + delayMillis * ONE_MILLIS_IN_NANOS;
        new Thread() {
            public void run() {
                LockTest.sleep(delayMillis);
                switch (type) {
                default:
                    locker.unlock();
                    break;
                case 1:
                    locker.unlockToShared();
                    break;
                case 2:
                    locker.unlockToUpgradable();
                    break;
                }
            }
        }.start();
        return end;
    }

    private Future<LockResult> tryLockShared(final Locker locker,
                                             final long indexId, final byte[] key)
    {
        return lockAsync(locker, indexId, key, 0);
    }

    private Future<LockResult> tryLockUpgradable(final Locker locker,
                                                 final long indexId, final byte[] key)
    {
        return lockAsync(locker, indexId, key, 1);
    }

    private Future<LockResult> tryLockExclusive(final Locker locker,
                                                final long indexId, final byte[] key)
    {
        return lockAsync(locker, indexId, key, 2);
    }

    private Future<LockResult> lockAsync(final Locker locker,
                                         final long indexId, final byte[] key,
                                         final int type)
    {
        return mExecutor.submit(new Callable<LockResult>() {
            public LockResult call() {
                LockResult result;
                synchronized (locker) {
                    switch (type) {
                    default:
                        result = locker.tryLockShared(indexId, key, MEDIUM_TIMEOUT);
                        break;
                    case 1:
                        result = locker.tryLockUpgradable(indexId, key, MEDIUM_TIMEOUT);
                        break;
                    case 2:
                        result = locker.tryLockExclusive(indexId, key, MEDIUM_TIMEOUT);
                        break;
                    }
                }
                return result;
            }
        });
    }
}