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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SplitSourceManager {
    private static final Logger LOG = LogManager.getLogger(SplitSourceManager.class);
    private static final ReferenceQueue<SplitSource> SPLITS_REF_QUEUE = new ReferenceQueue<>();
    private static final Map<Long, WeakReference<SplitSource>> SPLITS = new ConcurrentHashMap<>();

    static {
        Thread cleanerThread = new Thread(() -> {
            while (true) {
                try {
                    SplitSource splitSource = SPLITS_REF_QUEUE.remove().get();
                    if (splitSource != null) {
                        removeSplitSource(splitSource.getUniqueId());
                    } else {
                        SPLITS.entrySet().removeIf(entry -> {
                            if (entry.getValue().get() == null) {
                                LOG.warn("Remove splits source(is null)" + entry.getKey());
                                return true;
                            }
                            return false;
                        });
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to clean split source", e);
                }
            }
        }, "split-source-cleaner-thread");
        cleanerThread.setDaemon(true);
        cleanerThread.start();
    }

    public static void registerSplitSource(SplitSource splitSource) {
        SPLITS.put(splitSource.getUniqueId(), new WeakReference<>(splitSource, SPLITS_REF_QUEUE));
        LOG.warn("Register splits source " + splitSource.getUniqueId());
    }

    public static void removeSplitSource(long uniqueId) {
        SPLITS.remove(uniqueId);
        LOG.warn("Remove splits source " + uniqueId);
    }

    public static SplitSource getSplitSource(long uniqueId) {
        return SPLITS.get(uniqueId).get();
    }
}
