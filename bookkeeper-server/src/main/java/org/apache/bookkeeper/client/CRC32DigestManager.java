package org.apache.bookkeeper.client;

import java.util.zip.CRC32;

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


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

class CRC32DigestManager extends DigestManager {
    public CRC32DigestManager(long ledgerId) {
        super(ledgerId);
    }

    @Override
    int getMacCodeLength() {
        return 8;
    }    
    
    @Override
    Digest getDigest() {
        return CRC32Digest.RECYCLER.get();
    }

    private static class CRC32Digest implements Digest, ByteBufProcessor {
        private final Handle recyclerHandle;
        private final CRC32 crc;

        public CRC32Digest(Handle recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
            this.crc = new CRC32();
        }

        @Override
        public void getValue(ByteBuf buf) {
            buf.writeLong(crc.getValue());
        }

        @Override
        public void update(ByteBuf data) {
            data.forEachByte(this);
        }

        @Override
        public void update(ByteBuf data, int index, int length) {
            data.forEachByte(index, length, this);
        }

        @Override
        public boolean process(byte value) throws Exception {
            crc.update(value);
            return true;
        }

        @Override
        public void recycle() {
            crc.reset();
            RECYCLER.recycle(this, recyclerHandle);
        }

        private static final Recycler<CRC32Digest> RECYCLER = new Recycler<CRC32Digest>() {
            protected CRC32Digest newObject(Recycler.Handle handle) {
                return new CRC32Digest(handle);
            }
        };
    }
}
