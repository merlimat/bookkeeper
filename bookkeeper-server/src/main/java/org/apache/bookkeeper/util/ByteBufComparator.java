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
package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class ByteBufComparator {

    /**
     * Compare a buffer with a portion of another buffer.
     * 
     * Equivalent to: bufferA.equals(bufferB.slice(bIndex, bLen)) but doesn't need to allocate a SlicedByteBuf object
     * 
     * @param bufferA
     *            Buffer a
     * @param bufferB
     *            Buffer b
     * @param bIndex
     *            index inside buffer b
     * @param bLen
     *            length to compare in buffer b
     * @return true if buffer a is equal to the slice of buffer b
     */
    public static boolean equals(ByteBuf bufferA, ByteBuf bufferB, int bIndex, int bLen) {
        final int aLen = bufferA.readableBytes();
        if (aLen != bLen) {
            return false;
        }

        final int longCount = aLen >>> 3;
        final int byteCount = aLen & 7;

        int aIndex = bufferA.readerIndex();

        if (bufferA.order() == bufferB.order()) {
            for (int i = longCount; i > 0; i--) {
                if (bufferA.getLong(aIndex) != bufferB.getLong(bIndex)) {
                    return false;
                }
                aIndex += 8;
                bIndex += 8;
            }
        } else {
            for (int i = longCount; i > 0; i--) {
                if (bufferA.getLong(aIndex) != ByteBufUtil.swapLong(bufferB.getLong(bIndex))) {
                    return false;
                }
                aIndex += 8;
                bIndex += 8;
            }
        }

        for (int i = byteCount; i > 0; i--) {
            if (bufferA.getByte(aIndex) != bufferB.getByte(bIndex)) {
                return false;
            }
            aIndex++;
            bIndex++;
        }

        return true;
    }
}
