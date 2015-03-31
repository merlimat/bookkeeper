/**
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
package org.apache.bookkeeper.client;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntHashSet;
import com.google.common.base.Objects;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

/**
 * A specific {@link DistributionSchedule} that places entries in round-robin
 * fashion. For ensemble size 3, and quorum size 2, Entry 0 goes to bookie 0 and
 * 1, entry 1 goes to bookie 1 and 2, and entry 2 goes to bookie 2 and 0, and so
 * on.
 *
 */
class RoundRobinDistributionSchedule implements DistributionSchedule {
    private final int writeQuorumSize;
    private final int ackQuorumSize;
    private final int ensembleSize;

    private final IntArrayList[] writeSets;

    public RoundRobinDistributionSchedule(int writeQuorumSize, int ackQuorumSize, int ensembleSize) {
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.ensembleSize = ensembleSize;
        
        // Pre-compute possible write sets
        writeSets = new IntArrayList[ensembleSize];
        for (int i = 0; i < ensembleSize; i++) {
            writeSets[i] = new IntArrayList(writeQuorumSize);
            for (int w = 0; w < this.writeQuorumSize; w++) {
                writeSets[i].add((i + w) % ensembleSize);
            }
        }
    }

    @Override
    public IntArrayList getWriteSet(long entryId) {
        return writeSets[(int) (entryId % ensembleSize)];
    }

    @Override
    public AckSet getAckSet() {
        return AckSetImpl.create(ackQuorumSize);
    }

    private static class AckSetImpl implements AckSet {
        private int ackQuorumSize;
        private IntHashSet set = new IntHashSet();

        private final Handle recyclerHandle;
        private static final Recycler<AckSetImpl> RECYCLER = new Recycler<AckSetImpl>() {
            protected AckSetImpl newObject(Recycler.Handle handle) {
                return new AckSetImpl(handle);
            }
        };

        private AckSetImpl(Handle recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static AckSetImpl create(int ackQuorumSize) {
            AckSetImpl ackSet = RECYCLER.get();
            ackSet.ackQuorumSize = ackQuorumSize;
            ackSet.set.clear();
            return ackSet;
        }

        @Override
        public boolean addBookieAndCheck(int bookieIndexHeardFrom) {
            set.add(bookieIndexHeardFrom);
            return set.size() >= ackQuorumSize;
        }

        @Override
        public void removeBookie(int bookie) {
            set.removeAll(bookie);
        }

        @Override
        public void recycle() {
            RECYCLER.recycle(this, recyclerHandle);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("ackQuorumSize", ackQuorumSize).add("set", set).toString();
        }
    }

    private class RRQuorumCoverageSet implements QuorumCoverageSet {
        private final boolean[] covered = new boolean[ensembleSize];

        private RRQuorumCoverageSet() {
            for (int i = 0; i < covered.length; i++) {
                covered[i] = false;
            }
        }

        public synchronized boolean addBookieAndCheckCovered(int bookieIndexHeardFrom) {
            covered[bookieIndexHeardFrom] = true;

            // now check if there are any write quorums, with |ackQuorum| nodes available
            for (int i = 0; i < ensembleSize; i++) {
                int nodesNotCovered = 0;
                for (int j = 0; j < writeQuorumSize; j++) {
                    int nodeIndex = (i + j) % ensembleSize;
                    if (!covered[nodeIndex]) {
                        nodesNotCovered++;
                    }
                }
                if (nodesNotCovered >= ackQuorumSize) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public QuorumCoverageSet getCoverageSet() {
        return new RRQuorumCoverageSet();
    }
    
    @Override
    public boolean hasEntry(long entryId, int bookieIndex) {
        return getWriteSet(entryId).contains(bookieIndex);
    }
}
