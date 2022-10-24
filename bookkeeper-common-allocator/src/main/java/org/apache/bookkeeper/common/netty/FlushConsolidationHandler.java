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
 */
package org.apache.bookkeeper.common.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FlushConsolidationHandler extends ChannelDuplexHandler {

    private final int maxOperations;
    private final int maxBytes;
    private final long maxWaitTimeMicros;

    public static class Builder {

        private int maxOperations = 1000;
        private int maxBytes = 1024 * 1024;
        private long maxWaitTimeMicros = 1_000;

        public Builder maxBytes(int maxBytes) {
            this.maxBytes = maxBytes;
            return this;
        }

        public Builder maxOperations(int maxOperations) {
            this.maxOperations = maxOperations;
            return this;
        }

        public Builder maxWaitTime(int maxWaitTime, TimeUnit unit) {
            this.maxWaitTimeMicros = unit.toMicros(maxWaitTime);
            return this;
        }

        public FlushConsolidationHandler build() {
            return new FlushConsolidationHandler(maxOperations, maxBytes, maxWaitTimeMicros);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private ChannelHandlerContext ctx;
    private final Runnable flushTask;

    private int pendingOps;
    private int pendingBytes;
    private Future<?> nextScheduledFlush;

    private FlushConsolidationHandler(int maxOperations, int maxBytes, long maxWaitTimeMicros) {
        this.maxOperations = maxOperations;
        this.maxBytes = maxBytes;
        this.maxWaitTimeMicros = maxWaitTimeMicros;
        this.pendingBytes = 0;

        this.flushTask = () -> {
            if (pendingOps > 0) {
                pendingOps = 0;
                pendingBytes = 0;
                nextScheduledFlush = null;
                ctx.flush();
            }
        };
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        super.write(ctx, msg, promise);

        ++pendingOps;
        if (msg instanceof ByteBuf) {
            pendingBytes += ((ByteBuf) msg).readableBytes();
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        if (pendingOps > maxOperations || pendingBytes > maxBytes) {
            flushNow(ctx);
        } else {
            scheduleFlush(ctx);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cancelScheduledFlush();
        super.channelInactive(ctx);
    }

    private void flushNow(ChannelHandlerContext ctx) {
        cancelScheduledFlush();
        pendingOps = 0;
        pendingBytes = 0;
        ctx.flush();
    }

    private void scheduleFlush(final ChannelHandlerContext ctx) {
        if (nextScheduledFlush == null) {
            // Run as soon as possible, but still yield to give a chance for additional writes to enqueue.
            if (maxWaitTimeMicros > 0) {
                nextScheduledFlush =
                        ctx.channel().eventLoop().schedule(flushTask, maxWaitTimeMicros, TimeUnit.MICROSECONDS);
            } else {
                nextScheduledFlush = ctx.channel().eventLoop().submit(flushTask);
            }
        }
    }

    private void cancelScheduledFlush() {
        if (nextScheduledFlush != null) {
            nextScheduledFlush.cancel(false);
            nextScheduledFlush = null;
        }
    }
}
