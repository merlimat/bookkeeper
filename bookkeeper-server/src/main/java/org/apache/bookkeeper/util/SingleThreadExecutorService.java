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

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

public class SingleThreadExecutorService {

    private final BlockingMessagePassingQueue<Runnable> queue;
    private volatile boolean running;
    private final Thread thread;

    public SingleThreadExecutorService(ThreadFactory threadFactory) {
        this.queue = BlockingMessagePassingQueue.newMpscWithFixedSize(16 * 1024);
        this.running = true;

        this.thread = threadFactory.newThread(new Worker());
        this.thread.start();
    }

    public long getThreadId() {
        return thread.getId();
    }

    public void execute(Runnable command) {
        try {
            queue.put(command);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new RejectedExecutionException(e);
        }

        // Ensure that exception is thrown when a task is being submitted and the executor is shutting down.
        if (!running) {
            throw new RejectedExecutionException("Executor already closed");
        }
    }

    public void shutdown() {
        running = false;
        thread.interrupt();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        thread.join(unit.toMillis(timeout));
        return !thread.isAlive();
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            while (running) {
                Runnable runnable;
                try {
                    runnable = queue.take();
                } catch (InterruptedException e) {
                    // Next iteration, 'running' should be false
                    break;
                }

                try {
                    runnable.run();
                } catch (Throwable t) {
                    log.error("Exception in executor: {}", t.getMessage(), t);
                }
            }

            log.info("Executor worker is done");
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("thread", thread.getName()).toString();
    }

    private static final Logger log = LoggerFactory.getLogger(SingleThreadExecutorService.class);
}
