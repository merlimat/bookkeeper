/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.bookkeeper.common.collections.GrowableArrayBlockingQueue;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.jctools.queues.MpscBlockingConsumerArrayQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmarks for different blocking queue implementations.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(16)
@Fork(1)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
public class BlockingQueueBenchmark {

    private static final int MAX_QUEUE_SIZE = 100_000;

    private static Map<String, Supplier<BlockingQueue<MyEvent>>> queues = ImmutableMap.of(
            "LinkedBlockingQueue", () -> new LinkedBlockingQueue<>(),
            "ArrayBlockingQueue", () -> new ArrayBlockingQueue<>(MAX_QUEUE_SIZE),
            "DisruptorBlockingQueue", () -> new DisruptorBlockingQueue<>(MAX_QUEUE_SIZE),
            "MpscBlockingConsumerArrayQueue", () -> new MpscBlockingConsumerArrayQueue<>(MAX_QUEUE_SIZE),
            "GrowableArrayBlockingQueue", () -> new GrowableArrayBlockingQueue<>(MAX_QUEUE_SIZE)
    );

    private static class MyEvent {
        int a;
        int b;
    }

    private static final MyEvent myEvent = new MyEvent();

    /**
     * State holder of the test.
     */
    @State(Scope.Benchmark)
    public static class TestState {
        @Param({
                "LinkedBlockingQueue",
                "ArrayBlockingQueue",
                "DisruptorBlockingQueue",
                "MpscBlockingConsumerArrayQueue",
                "GrowableArrayBlockingQueue",
        })
        private String queueName;

        private BlockingQueue<MyEvent> queue;
        private ExecutorService executor = Executors.newSingleThreadExecutor();

        @Setup(Level.Trial)
        public void setup(Blackhole bh) {
            queue = queues.get(queueName).get();
            executor.execute(() -> {
                try {
                    while (true) {
                        MyEvent e = queue.take();
                        bh.consume(e);
                    }
                } catch (InterruptedException ex) {
                   return;
                }
            });
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            executor.shutdownNow();
        }
    }

    @Benchmark
    public void push(TestState s) throws Exception {
        s.queue.put(myEvent);
    }
}
