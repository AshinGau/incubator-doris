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

package org.apache.doris.hudi;


import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ScanPredicate;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.util.WeakIdentityHashMap;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import scala.collection.Iterator;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * The hudi JniScanner
 */
public class HudiJniScanner extends JniScanner {
    private static final Logger LOG = Logger.getLogger(HudiJniScanner.class);

    private final int fetchSize;
    private final String debugString;
    private final HoodieSplit split;
    private final ScanPredicate[] predicates;
    private final ClassLoader classLoader;
    private final UserGroupInformation ugi;

    private long getRecordReaderTimeNs = 0;
    private Iterator<InternalRow> recordIterator;

    private static final ExecutorService avroReadPool;
    private static ThreadLocal<WeakIdentityHashMap<?, ?>> AVRO_RESOLVER_CACHE;
    private static final Map<Long, WeakIdentityHashMap<?, ?>> cachedResolvers = new ConcurrentHashMap<>();
    private static final ReadWriteLock cleanResolverLock = new ReentrantReadWriteLock();
    private static final ScheduledExecutorService cleanResolverService = Executors.newScheduledThreadPool(1);

    static {
        int numThreads = Math.max(Runtime.getRuntime().availableProcessors() * 2 + 1, 4);
        avroReadPool = Executors.newFixedThreadPool(numThreads,
                new ThreadFactoryBuilder().setNameFormat("avro-log-reader-%d").build());
        LOG.info("Create " + numThreads + " daemon threads to load avro logs");

        Class<?> avroReader = GenericDatumReader.class;
        try {
            Field field = avroReader.getDeclaredField("RESOLVER_CACHE");
            field.setAccessible(true);
            AVRO_RESOLVER_CACHE = (ThreadLocal<WeakIdentityHashMap<?, ?>>) field.get(null);
            LOG.info("Get the resolved cache for avro reader");
        } catch (Exception e) {
            AVRO_RESOLVER_CACHE = null;
            LOG.warn("Failed to get the resolved cache for avro reader");
        }

        cleanResolverService.scheduleAtFixedRate(() -> {
            LOG.warn("Start GC, avro readers : " + cachedResolvers.size());
            System.gc();
            cleanResolverLock.writeLock().lock();
            LOG.warn("Start reap...");
            try {
                for (Map.Entry<Long, WeakIdentityHashMap<?, ?>> solver : cachedResolvers.entrySet()) {
                    LOG.warn("Avro reader of thread " + solver.getKey() + " cached " + solver.getValue().size()
                            + " resolves after full GC.");
                    Field weakQueue = solver.getValue().getClass().getDeclaredField("queue");
                    weakQueue.setAccessible(true);
                    ReferenceQueue<?> queue = (ReferenceQueue<?>) weakQueue.get(solver.getValue());
                    Field queueLength = queue.getClass().getDeclaredField("queueLength");
                    queueLength.setAccessible(true);
                    long length = (Long) queueLength.get(queue);
                    LOG.warn("Avro reader of thread " + solver.getKey() + " weak queue size: " + length);
                }
            } catch (Exception e) {
                LOG.warn("Failed to clean avro reader", e);
            } finally {
                cleanResolverLock.writeLock().unlock();
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    public HudiJniScanner(int fetchSize, Map<String, String> params) {
        debugString = params.entrySet().stream().map(kv -> kv.getKey() + "=" + kv.getValue())
                .collect(Collectors.joining("\n"));
        try {
            this.classLoader = this.getClass().getClassLoader();
            String predicatesAddressString = params.remove("push_down_predicates");
            this.fetchSize = fetchSize;
            this.split = new HoodieSplit(params);
            if (predicatesAddressString == null) {
                predicates = new ScanPredicate[0];
            } else {
                long predicatesAddress = Long.parseLong(predicatesAddressString);
                if (predicatesAddress != 0) {
                    predicates = ScanPredicate.parseScanPredicates(predicatesAddress, split.requiredTypes());
                    LOG.info("HudiJniScanner gets pushed-down predicates:  " + ScanPredicate.dump(predicates));
                } else {
                    predicates = new ScanPredicate[0];
                }
            }
            ugi = Utils.getUserGroupInformation(split.hadoopConf());
        } catch (Exception e) {
            LOG.error("Failed to initialize hudi scanner, split params:\n" + debugString, e);
            throw e;
        }
    }

    @Override
    public void open() throws IOException {
        Future<?> avroFuture = avroReadPool.submit(() -> {
            Thread.currentThread().setContextClassLoader(classLoader);
            initTableInfo(split.requiredTypes(), split.requiredFields(), predicates, fetchSize);
            long startTime = System.nanoTime();
            // RecordReader will use ProcessBuilder to start a hotspot process, which may be stuck,
            // so use another process to kill this stuck process.
            // TODO(gaoxin): better way to solve the stuck process?
            AtomicBoolean isKilled = new AtomicBoolean(false);
            ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
            executorService.scheduleAtFixedRate(() -> {
                if (!isKilled.get()) {
                    synchronized (HudiJniScanner.class) {
                        List<Long> pids = Utils.getChildProcessIds(
                                Utils.getCurrentProcId());
                        for (long pid : pids) {
                            String cmd = Utils.getCommandLine(pid);
                            if (cmd != null && cmd.contains("org.openjdk.jol.vm.sa.AttachMain")) {
                                Utils.killProcess(pid);
                                isKilled.set(true);
                                LOG.info("Kill hotspot debugger process " + pid);
                            }
                        }
                    }
                }
            }, 100, 1000, TimeUnit.MILLISECONDS);

            cleanResolverLock.readLock().lock();
            try {
                if (ugi != null) {
                    recordIterator = ugi.doAs(
                            (PrivilegedExceptionAction<Iterator<InternalRow>>) () -> new MORSnapshotSplitReader(
                                    split).buildScanIterator(split.requiredFields(), new Filter[0]));
                } else {
                    recordIterator = new MORSnapshotSplitReader(split)
                            .buildScanIterator(split.requiredFields(), new Filter[0]);
                }
            } catch (Exception e) {
                LOG.error("Failed to open hudi scanner, split params:\n" + debugString, e);
                throw new RuntimeException(e.getMessage(), e);
            } finally {
                cleanResolverLock.readLock().unlock();
            }
            isKilled.set(true);
            executorService.shutdownNow();
            if (AVRO_RESOLVER_CACHE != null && AVRO_RESOLVER_CACHE.get() != null) {
                cachedResolvers.computeIfAbsent(Thread.currentThread().getId(),
                        threadId -> AVRO_RESOLVER_CACHE.get());
                LOG.warn("Thread " + Thread.currentThread().getId() + " has resolvers: " + AVRO_RESOLVER_CACHE.get()
                        .size());
            }
            getRecordReaderTimeNs += System.nanoTime() - startTime;
        });
        try {
            avroFuture.get();
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (recordIterator instanceof Closeable) {
            ((Closeable) recordIterator).close();
        }
        recordIterator = null;
    }

    @Override
    public int getNext() throws IOException {
        try {
            int readRowNumbers = 0;
            HudiColumnValue columnValue = new HudiColumnValue();
            int numFields = split.requiredFields().length;
            ColumnType[] columnTypes = split.requiredTypes();
            while (readRowNumbers < fetchSize && recordIterator.hasNext()) {
                columnValue.reset(recordIterator.next());
                for (int i = 0; i < numFields; i++) {
                    columnValue.reset(i, columnTypes[i].getPrecision(), columnTypes[i].getScale());
                    appendData(i, columnValue);
                }
                readRowNumbers++;
            }
            return readRowNumbers;
        } catch (Exception e) {
            close();
            LOG.error("Failed to get the next batch of hudi, split params:\n" + debugString, e);
            throw new IOException("Failed to get the next batch of hudi.", e);
        }
    }

    @Override
    public Map<String, String> getStatistics() {
        return Collections.singletonMap("timer:GetRecordReaderTime", String.valueOf(getRecordReaderTimeNs));
    }
}
