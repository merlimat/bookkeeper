/**
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

package org.apache.bookkeeper.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscArrayQueue;

public class BlockingMessagePassingQueue<E> implements MessagePassingQueue<E> {

    private final MessagePassingQueue<E> queue;

    private final static long minWaitTimeNanos = TimeUnit.NANOSECONDS.toNanos(1);
    private final static long maxWaitTimeNanos = TimeUnit.MICROSECONDS.toNanos(100);

    public static <T> BlockingMessagePassingQueue<T> newMpscWithFixedSize(int size) {
        return new BlockingMessagePassingQueue<T>(new MpscArrayQueue<T>(size));
    }

    private BlockingMessagePassingQueue(MessagePassingQueue<E> queue) {
        this.queue = queue;
    }

    // Additional blocking queue methods

    /**
     * Inserts the specified element into this queue, waiting if necessary for space to become available.
     *
     * @param e
     *            the element to add
     * @throws ClassCastException
     *             if the class of the specified element prevents it from being added to this queue
     * @throws NullPointerException
     *             if the specified element is null
     * @throws IllegalArgumentException
     *             if some property of the specified element prevents it from being added to this queue
     */
    public void put(E e) throws InterruptedException {
        // Retry when the queue is full
        long currentWaitTimeNanos = minWaitTimeNanos;
        while (!queue.offer(e)) {
            LockSupport.parkNanos(currentWaitTimeNanos);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            currentWaitTimeNanos = nextWaitTime(currentWaitTimeNanos);
        }
    }

    /**
     * Retrieves and removes the head of this queue, waiting if necessary until an element becomes available.
     *
     * @return the head of this queue
     */
    public E take() throws InterruptedException {
        E entry = null;
        long currentWaitTimeNanos = minWaitTimeNanos;

        while (true) {
            entry = poll();
            if (entry != null) {
                return entry;
            }

            LockSupport.parkNanos(currentWaitTimeNanos);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            currentWaitTimeNanos = nextWaitTime(currentWaitTimeNanos);
        }
    }

    /**
     * Retrieves and removes the head of this queue, waiting up to the specified wait time if necessary for an element
     * to become available.
     *
     * @param timeout
     *            how long to wait before giving up, in units of {@code unit}
     * @param unit
     *            a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
     * @return the head of this queue, or {@code null} if the specified waiting time elapses before an element is
     *         available
     */
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long absoluteTimeout = System.nanoTime() + unit.toNanos(timeout);

        E entry = null;
        long currentWaitTimeNanos = minWaitTimeNanos;
        while (true) {
            entry = poll();
            if (entry != null) {
                return entry;
            }

            if (System.nanoTime() > absoluteTimeout) {
                // Queue still empty after timeout
                return null;
            }

            LockSupport.parkNanos(currentWaitTimeNanos);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            currentWaitTimeNanos = nextWaitTime(currentWaitTimeNanos);
        }
    }

    // Proxied methods

    public boolean offer(E e) {
        return queue.offer(e);
    }

    public E poll() {
        return queue.poll();
    }

    public E peek() {
        return queue.peek();
    }

    public int size() {
        return queue.size();
    }

    public void clear() {
        queue.clear();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int capacity() {
        return queue.capacity();
    }

    public boolean relaxedOffer(E e) {
        return queue.relaxedOffer(e);
    }

    public E relaxedPoll() {
        return queue.relaxedPoll();
    }

    public E relaxedPeek() {
        return queue.relaxedPeek();
    }

    public int drain(MessagePassingQueue.Consumer<E> c) {
        return queue.drain(c);
    }

    public int fill(MessagePassingQueue.Supplier<E> s) {
        return queue.fill(s);
    }

    public int drain(MessagePassingQueue.Consumer<E> c, int limit) {
        return queue.drain(c, limit);
    }

    public int fill(MessagePassingQueue.Supplier<E> s, int limit) {
        return queue.fill(s, limit);
    }

    public void drain(MessagePassingQueue.Consumer<E> c, MessagePassingQueue.WaitStrategy wait,
            MessagePassingQueue.ExitCondition exit) {
        queue.drain(c, wait, exit);
    }

    public void fill(MessagePassingQueue.Supplier<E> s, MessagePassingQueue.WaitStrategy wait,
            MessagePassingQueue.ExitCondition exit) {
        queue.fill(s, wait, exit);
    }

    private static long nextWaitTime(long currentWaitTimeNanos) {
        return Math.min(currentWaitTimeNanos * 2, maxWaitTimeNanos);
    }
}
