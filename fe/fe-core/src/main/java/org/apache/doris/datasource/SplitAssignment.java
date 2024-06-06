// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;

import com.google.common.collect.Multimap;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * When file splits are supplied in batch mode, splits are generated lazily and assigned in each call of `getNextBatch`.
 * `SplitGenerator` provides the file splits, and `FederationBackendPolicy` assigns these splits to backends.
 */
public class SplitAssignment {
    // magic number to estimate how many splits are allocated to BE in each batch
    private static final int NUM_SPLITS_PER_BE = 1024;
    // magic number to estimate how many splits are generated of each partition in each batch.
    private static final int NUM_SPLITS_PER_PARTITION = 10;
    private static final int NUM_PREFETCH_BATCHES = 4;

    private final Set<Long> sources = new HashSet<>();
    private final FederationBackendPolicy backendPolicy;
    private final SplitGenerator splitGenerator;
    private final AtomicBoolean isLastBatch = new AtomicBoolean(false);
    private final ConcurrentHashMap<Backend, BlockingQueue<Split>> assignment = new ConcurrentHashMap<>();
    private Split sampleSplit;
    private final int maxBatchSize;
    private final Executor executor;
    private final AtomicBoolean isStop = new AtomicBoolean(false);

    private UserException exception;

    public SplitAssignment(FederationBackendPolicy backendPolicy, SplitGenerator splitGenerator) {
        this.backendPolicy = backendPolicy;
        this.splitGenerator = splitGenerator;
        int numPartitions = ConnectContext.get().getSessionVariable().getNumPartitionsInBatchMode();
        maxBatchSize = Math.max(NUM_SPLITS_PER_PARTITION * numPartitions,
                NUM_SPLITS_PER_BE * backendPolicy.numBackends());
        executor = Env.getCurrentEnv().getExtMetaCacheMgr().getFileListingExecutor();
    }

    public void init() throws UserException {
        if (assignment.isEmpty() && splitGenerator.hasNext()) {
            Multimap<Backend, Split> batch = backendPolicy.computeScanRangeAssignment(
                    splitGenerator.getNextBatch(maxBatchSize));
            appendBatch(batch);
            sampleSplit = batch.values().iterator().next();
        }
        generateBatches();
        prefetchSplits();
    }

    private void appendBatch(Multimap<Backend, Split> batch) {
        for (Backend backend : batch.keySet()) {
            assignment.computeIfAbsent(backend, be -> new LinkedBlockingQueue<>()).addAll(batch.get(backend));
        }
    }

    public void registerSource(long uniqueId) {
        sources.add(uniqueId);
    }

    public Set<Long> getSources() {
        return sources;
    }

    public Split getSampleSplit() {
        return sampleSplit;
    }

    public int numApproximateSplits() {
        return splitGenerator.numApproximateSplits();
    }

    private void generateBatches() {
        executor.execute(() -> {
            try {
                while (splitGenerator.hasNext() && !isStop.get()) {
                    synchronized (this) {
                        wait(1000);
                    }
                    for (int i = 0; i < NUM_PREFETCH_BATCHES && splitGenerator.hasNext() && !isStop.get(); ++i) {
                        appendBatch(backendPolicy.computeScanRangeAssignment(
                                splitGenerator.getNextBatch(maxBatchSize)));
                    }
                }
                isLastBatch.set(true);
            } catch (UserException e) {
                exception = e;
            } catch (InterruptedException e) {
                exception = new UserException("Interrupt in generate split batches", e);
            }
        });
    }

    public BlockingQueue<Split> getAssignedSplits(Backend backend) throws UserException {
        if (exception != null) {
            throw exception;
        }
        BlockingQueue<Split> splits = assignment.computeIfAbsent(backend, be -> new LinkedBlockingQueue<>());
        if (isLastBatch.get() && splits.isEmpty()) {
            return null;
        }
        if (splits.size() < NUM_SPLITS_PER_BE) {
            prefetchSplits();
        }
        return splits;
    }

    public synchronized void prefetchSplits() {
        notify();
    }

    public synchronized void stop() {
        isStop.set(true);
        notify();
    }
}
