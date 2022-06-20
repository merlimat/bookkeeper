/*
 *
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
 *
 */
package org.apache.bookkeeper.common.collections;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import jdk.internal.vm.annotation.Contended;
import org.apache.bookkeeper.util.MathUtils;


/**
 * This implements a {@link BlockingQueue} backed by an array with no fixed capacity.
 *
 * <p>When the capacity is reached, data will be moved to a bigger array.
 *
 */
public class GrowableMpScArrayConsumerBlockingQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {

    private final StampedLock consumerLock = new StampedLock();
    @Contended
    private final PaddedInt consumerIdx = new PaddedInt();
    @Contended
    private final AtomicReference<Thread> waitingConsumer = new AtomicReference<>();

    @Contended
    private final AtomicInteger producerIdx = new AtomicInteger();
    private final StampedLock producerLock = new StampedLock();
    private int producerLimit;
    private int mask;

    private AtomicReferenceArray<T> data;

    public GrowableMpScArrayConsumerBlockingQueue() {
        this(64);
    }

    public GrowableMpScArrayConsumerBlockingQueue(int initialCapacity) {
        int capacity = MathUtils.findNextPositivePowerOfTwo(initialCapacity);
        this.data = new AtomicReferenceArray<>(capacity);
        this.mask = capacity - 1;
        this.producerLimit = capacity;

        producerIdx.set(0);
        consumerIdx.value = 0;
    }

    @Override
    public boolean offer(T t) {
        // The queue has no limit, so the offer always succeeds
        put(t);
        return true;
    }

    @Override
    public T poll() {
        int consumerIdx = this.consumerIdx.value;

        long stamp = consumerLock.tryOptimisticRead();

        T value = data.getAndSet(consumerIdx, null);

        if (value != null && consumerLock.validate(stamp)) {
            this.consumerIdx.value = (consumerIdx + 1) & (data.length() - 1);
            return value;
        }

        // Acquire regular read lock. There will be no array expansions possible at this point
        stamp = consumerLock.readLock();
        try {
            value = data.getAndSet(consumerIdx, null);
            if (value == null) {
                // The value could be null when the producer still has not updated the array after incrementing the idx
                if (consumerIdx != producerIdx.get()) {
                    do {
                        value = data.getAndSet(consumerIdx, null);
                    } while (value == null);
                }
            }

            if (value != null) {
                this.consumerIdx.value = (consumerIdx + 1) & (data.length() - 1);
            }

            return value;
        } finally {
            consumerLock.unlockRead(stamp);
        }
    }

    private int _size() {
        int producerIdx = this.producerIdx.get();
        int consumerIdx = this.consumerIdx.value;
        if (consumerIdx <= producerIdx) {
            return producerIdx - consumerIdx;
        } else {
            return producerIdx;
        }
    }

    @Override
    public T peek() {
        int consumerIdx = this.consumerIdx.value;

        long stamp = consumerLock.tryOptimisticRead();

        T value = data.get(consumerIdx);

        if (value != null && consumerLock.validate(stamp)) {
            return value;
        }

        // Acquire regular read lock. There will be no array expansions possible at this point
        stamp = consumerLock.readLock();
        try {
            value = data.get(consumerIdx);
            if (value == null) {
                // The value could be null when the producer still has not updated the array after incrementing the idx
                if (consumerIdx != producerIdx.get()) {
                    do {
                        value = data.get(consumerIdx);
                    } while (value == null);
                }
            }

            return value;
        } finally {
            consumerLock.unlockRead(stamp);
        }
    }

    @Override
    public void put(T e) {
        long stamp = producerLock.tryOptimisticRead();

        int producerIdx = this.producerIdx.incrementAndGet() & mask;
        int consumerIdx = this.consumerIdx.value;

        Thread waitingConsumer = null;
        if (producerIdx == consumerIdx) {
            waitingConsumer = this.waitingConsumer.getAndSet(null);
        }

        data.set(producerIdx, e);
//
//        if (!producerLock.validate(stamp)) {
//            stamp = producerLock.readLock();
//
//            try {
//                producerIdx = this.producerIdx.incrementAndGet() & mask;
//                consumerIdx = this.consumerIdx.value;
//
//                if (pr)
//
//            } finally {
//                producerLock.unlockRead(readStamp);
//            }
//        }


        if (waitingConsumer != null) {
            LockSupport.unpark(waitingConsumer);
        }

//        if (nextIdx)

//        boolean wasEmpty = false;
//
//        try {
//            if (SIZE_UPDATER.get(this) == data.length) {
//                expandArray();
//            }
//
//            data[tailIndex.value] = e;
//            tailIndex.value = (tailIndex.value + 1) & (data.length - 1);
//            if (SIZE_UPDATER.getAndIncrement(this) == 0) {
//                wasEmpty = true;
//            }
//        } finally {
//            producerLock.unlockRead(readStamp);
//        }
//
//        if (wasEmpty) {
//            headLock.lock();
//            try {
//                isNotEmpty.signal();
//            } finally {
//                headLock.unlock();
//            }
//        }
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) {
        // Queue is unbounded and it will never reject new items
        put(e);
        return true;
    }

    @Override
    public T take() throws InterruptedException {
//        consumerLock.readLockInterruptibly();

//        try {
//            while (SIZE_UPDATER.get(this) == 0) {
//                isNotEmpty.await();
//            }
//
//            T item = data[headIndex.value];
//            data[headIndex.value] = null;
//            headIndex.value = (headIndex.value + 1) & (data.length - 1);
//            if (SIZE_UPDATER.decrementAndGet(this) > 0) {
//                // There are still entries to consume
//                isNotEmpty.signal();
//            }
//            return item;
//        } finally {
//            headLock.unlock();
//        }
        return null;
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
//        headLock.lockInterruptibly();
//
//        try {
//            long timeoutNanos = unit.toNanos(timeout);
//            while (SIZE_UPDATER.get(this) == 0) {
//                if (timeoutNanos <= 0) {
//                    return null;
//                }
//
//                timeoutNanos = isNotEmpty.awaitNanos(timeoutNanos);
//            }
//
//            T item = data[headIndex.value];
//            data[headIndex.value] = null;
//            headIndex.value = (headIndex.value + 1) & (data.length - 1);
//            if (SIZE_UPDATER.decrementAndGet(this) > 0) {
//                // There are still entries to consume
//                isNotEmpty.signal();
//            }
//            return item;
//        } finally {
//            headLock.unlock();
//        }
        return null;
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
//        headLock.lock();
//
//        try {
//            int drainedItems = 0;
//            int size = SIZE_UPDATER.get(this);
//
//            while (size > 0 && drainedItems < maxElements) {
//                T item = data[headIndex.value];
//                data[headIndex.value] = null;
//                c.add(item);
//
//                headIndex.value = (headIndex.value + 1) & (data.length - 1);
//                --size;
//                ++drainedItems;
//            }
//
//            if (SIZE_UPDATER.addAndGet(this, -drainedItems) > 0) {
//                // There are still entries to consume
//                isNotEmpty.signal();
//            }
//
//            return drainedItems;
//        } finally {
//            headLock.unlock();
//        }
        return 0;
    }

    @Override
    public void clear() {
//        headLock.lock();
//
//        try {
//            int size = SIZE_UPDATER.get(this);
//
//            for (int i = 0; i < size; i++) {
//                data[headIndex.value] = null;
//                headIndex.value = (headIndex.value + 1) & (data.length - 1);
//            }
//
//            if (SIZE_UPDATER.addAndGet(this, -size) > 0) {
//                // There are still entries to consume
//                isNotEmpty.signal();
//            }
//        } finally {
//            headLock.unlock();
//        }
    }

    @Override
    public int size() {
//        return SIZE_UPDATER.get(this);
        return 0;
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

//        long tailStamp = tailLock.writeLock();
//        headLock.lock();
//
//        try {
//            int headIndex = this.headIndex.value;
//            int size = SIZE_UPDATER.get(this);
//
//            sb.append('[');
//
//            for (int i = 0; i < size; i++) {
//                T item = data[headIndex];
//                if (i > 0) {
//                    sb.append(", ");
//                }
//
//                sb.append(item);
//
//                headIndex = (headIndex + 1) & (data.length - 1);
//            }
//
//            sb.append(']');
//        } finally {
//            headLock.unlock();
//            tailLock.unlockWrite(tailStamp);
//        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private void expandArray(long producerStamp) {
        // We already hold the producer lock for read
//        long producerStamp = producerLock.tryConvertToWriteLock(0);
//        headLock.lock();
//
//        try {
//            int size = SIZE_UPDATER.get(this);
//            int newCapacity = data.length * 2;
//            T[] newData = (T[]) new Object[newCapacity];
//
//            int oldHeadIndex = headIndex.value;
//            int newTailIndex = 0;
//
//            for (int i = 0; i < size; i++) {
//                newData[newTailIndex++] = data[oldHeadIndex];
//                oldHeadIndex = (oldHeadIndex + 1) & (data.length - 1);
//            }
//
//            data = newData;
//            headIndex.value = 0;
//            tailIndex.value = size;
//        } finally {
//            headLock.unlock();
//        }
    }

    static final class PaddedInt {
        private volatile int value;

        // Padding to avoid false sharing
        public volatile int pi1 = 1;
        public volatile long p1 = 1L, p2 = 2L, p3 = 3L, p4 = 4L, p5 = 5L, p6 = 6L;

        public long exposeToAvoidOptimization() {
            return pi1 + p1 + p2 + p3 + p4 + p5 + p6;
        }
    }

}
