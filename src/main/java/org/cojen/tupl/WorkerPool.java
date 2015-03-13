/*
 *  Copyright 2015 Brian S O'Neill
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
 * 
 *
 * @author Brian S O'Neill
 */
class WorkerPool {
    private final int mMaxThreads;

    private Task mHead;
    private Task mTail;

    WorkerPool(int maxThreads) {
        mMaxThreads = maxThreads;
    }

    public void init() {
        for (int i=0; i<mMaxThreads; i++) {
            new Worker().start();
        }
    }

    public void close() {
        enqueue(Task.TERMINATOR);
    }

    synchronized void enqueue(Task f) {
        Task tail = mTail;
        if (tail == null) {
            mHead = f;
        } else {
            tail.mNext = f;
        }
        mTail = f;
        notify();
    }

    synchronized Task dequeue() {
        Task head;
        while ((head = mHead) == null) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }

        Task next = head.mNext;
        head.mNext = null;
        mHead = next;
        if (next == null) {
            mTail = null;
        }

        return head;
    }

    static class Task {
        static final Task TERMINATOR = new Task();

        Task mNext;

        Task() {
        }

        /**
         * @return false if thread terminator
         */
        protected boolean runTask() {
            return false;
        }
    }

    final class Worker extends Thread {
        @Override
        public void run() {
            while (dequeue().runTask());
            // Hand off to the next thread.
            enqueue(Task.TERMINATOR);
        }
    }
}
