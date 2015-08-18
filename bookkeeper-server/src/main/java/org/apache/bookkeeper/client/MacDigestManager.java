package org.apache.bookkeeper.client;

import static com.google.common.base.Charsets.UTF_8;
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

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MacDigestManager extends DigestManager {
    private final static Logger LOG = LoggerFactory.getLogger(MacDigestManager.class);

    public static String DIGEST_ALGORITHM = "SHA-1";
    public static String KEY_ALGORITHM = "HmacSHA1";

    public static final int MAC_CODE_LENGTH = 20;

    final byte[] passwd;

    public MacDigestManager(long ledgerId, byte[] passwd) throws GeneralSecurityException {
        super(ledgerId);
        this.passwd = passwd;
    }

    public static byte[] genDigest(String pad, byte[] passwd) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance(DIGEST_ALGORITHM);
        digest.update(pad.getBytes(UTF_8));
        digest.update(passwd);
        return digest.digest();
    }

    @Override
    int getMacCodeLength() {
        return MAC_CODE_LENGTH;
    }

    @Override
    Digest getDigest() {
        return RECYCLER.get();
    }

    private class MACDigest implements Digest, ByteBufProcessor {
        private final Handle recyclerHandle;
        private final Mac mac;

        public MACDigest(Handle recyclerHandle) throws GeneralSecurityException {
            this.recyclerHandle = recyclerHandle;
            byte[] macKey = genDigest("mac", passwd);
            SecretKeySpec keySpec = new SecretKeySpec(macKey, KEY_ALGORITHM);
            this.mac = Mac.getInstance(KEY_ALGORITHM);
            this.mac.init(keySpec);
        }

        @Override
        public void getValue(ByteBuf buf) {
            buf.writeBytes(mac.doFinal());
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
            mac.update(value);
            return true;
        }

        @Override
        public void recycle() {
            mac.reset();
            RECYCLER.recycle(this, recyclerHandle);
        }
    }

    private final Recycler<MACDigest> RECYCLER = new Recycler<MACDigest>() {
        protected MACDigest newObject(Recycler.Handle handle) {
            try {
                return new MACDigest(handle);
            } catch (GeneralSecurityException gse) {
                LOG.error("Couldn't not get mac instance", gse);
                return null;
            }
        }
    };
}
