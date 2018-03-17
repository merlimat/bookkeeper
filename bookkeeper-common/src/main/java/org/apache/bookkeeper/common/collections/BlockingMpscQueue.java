/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.common.collections;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.jctools.queues.MpscLinkedQueue8;

/**
 * Blocking queue optimized for multiple producers and single consumer.
 */
public class BlockingMpscQueue<T> extends MpscLinkedQueue8<T> implements BlockingQueue<T> {

    public BlockingMpscQueue(int size) {
        super();
    }

    @Override
    public void put(T e) throws InterruptedException {
        while (!this.relaxedOffer(e)) {
            // Do busy-spin loop
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
        long absoluteEndTime = System.nanoTime() + unit.toNanos(timeout);

        while (!this.relaxedOffer(e)) {
            // Do busy-spin loop

            if (System.nanoTime() > absoluteEndTime) {
                return false;
            }

            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }

        return true;
    }

    @Override
    public T take() throws InterruptedException {
        int idleCounter = 0;
        while (true) {
            T item = relaxedPoll();
            if (item == null) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }

                idleCounter = WAIT_STRATEGY.idle(idleCounter);
                continue;
            }


            return item;
        }
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        long absoluteEndTime = System.nanoTime() + unit.toNanos(timeout);

        int idleCounter = 0;
        while (true) {
            T item = relaxedPoll();
            if (item == null) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }

                if (System.nanoTime() > absoluteEndTime) {
                    return null;
                } else {
                    idleCounter = WAIT_STRATEGY.idle(idleCounter);
                    continue;
                }
            }

            return item;
        }
    }

    @Override
    public int remainingCapacity() {
        return capacity() - size();
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        int initialSize = c.size();
        drain(c::add, WAIT_STRATEGY, EXIT_CONDITION);
        return c.size() - initialSize;
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        int initialSize = c.size();
        drain(c::add, WAIT_STRATEGY, EXIT_CONDITION);
        return c.size() - initialSize;
    }

    /**
     * Waiting strategy that starts with busy loop and gradually falls back to sleeping if no items are available.
     */
    private static final WaitStrategy WAIT_STRATEGY = new WaitStrategy() {

        private static final int BUSY_LOOP_LIMIT = 10;
        private static final int YIELD_LIMIT = BUSY_LOOP_LIMIT + 3;
        private static final int SHORT_SLEEP_LIMIT = YIELD_LIMIT + 3;

        private static final int SHORT_SLEEP_NANOS = 1;
        private static final long LONG_SLEEP_NANOS = 1_000;

        @Override
        public int idle(int idleCounter) {
            if (idleCounter < BUSY_LOOP_LIMIT) {
                // Initially, we do busy loop, to avoid interrupting the current thread when there is contention
                return idleCounter + 1;

            } else if (idleCounter < YIELD_LIMIT) {
                // Try to give precedence to other threads without de-scheduling the current thread
                Thread.yield();
                return idleCounter + 1;

            } else if (idleCounter < SHORT_SLEEP_LIMIT) {
                // Pause the thread for the shortest possible amount of time. We're asking for 1nanosecond, though in
                // reality the sleep will be closer to 60micros.
                LockSupport.parkNanos(SHORT_SLEEP_NANOS);
                return idleCounter + 1;

            } else {
                // Do a longer sleep to spare CPU utilization
                LockSupport.parkNanos(LONG_SLEEP_NANOS);

                // No need to increment the counter since we would not change the strategy anymore at this point. Also,
                // we need to be careful in not overflowing the integer.
                return idleCounter;
            }
        }
    };

    /**
     * Exit condition that keeps accepting data when draining into a collection.
     */
    private static final ExitCondition EXIT_CONDITION = () -> true;
}
