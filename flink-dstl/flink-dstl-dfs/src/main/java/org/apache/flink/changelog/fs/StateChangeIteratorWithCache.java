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

package org.apache.flink.changelog.fs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.configuration.StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL;

/** StateChangeIterator with local cache. */
class StateChangeIteratorWithCache extends StateChangeIteratorImpl {
    private static final Logger LOG = LoggerFactory.getLogger(StateChangeIteratorWithCache.class);

    private static final String CACHE_FILE_PREFIX = "dstl-";

    private final File cacheDir;
    private final ConcurrentMap<Path, FileCache> cache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cacheCleanScheduler;
    private final ExecutorService downloadExecutor;
    private final long cacheIdleMillis;

    StateChangeIteratorWithCache(ExecutorService downloadExecutor, Configuration config) {
        // TODO: 2022/5/31 add a new options for cache idle
        long cacheIdleMillis = config.get(PERIODIC_MATERIALIZATION_INTERVAL).toMillis();
        File cacheDir = ConfigurationUtils.getRandomTempDirectory(config);

        this.cacheCleanScheduler =
                SchedulerFactory.create(1, "ChangelogCacheFileCleanScheduler", LOG);
        this.downloadExecutor = downloadExecutor;
        this.cacheIdleMillis = cacheIdleMillis;
        this.cacheDir = cacheDir;
    }

    @Override
    public CloseableIterator<StateChange> read(StreamStateHandle handle, long offset)
            throws IOException {

        if (!(handle instanceof FileStateHandle)) {
            return new StateChangeFormat().read(wrapAndSeek(handle.openInputStream(), offset));
        }

        FileStateHandle fileHandle = (FileStateHandle) handle;
        DataInputStream input;

        if (fileHandle.getFilePath().getFileSystem().isDistributedFS()) {

            Path dfsPath = fileHandle.getFilePath();
            FileCache fileCache =
                    cache.computeIfAbsent(
                            dfsPath,
                            key -> {
                                FileCache fCache = new FileCache(cacheDir);
                                downloadExecutor.execute(() -> downloadFile(fileHandle, fCache));
                                return fCache;
                            });

            FileInputStream fin = fileCache.openAndSeek(offset);

            input =
                    new DataInputStream(new BufferedInputStream(fin)) {
                        @Override
                        public void close() throws IOException {
                            super.close();
                            if (fileCache.getRefCount() == 0) {
                                cacheCleanScheduler.schedule(
                                        () -> cleanFileCache(dfsPath, fileCache),
                                        cacheIdleMillis,
                                        TimeUnit.MILLISECONDS);
                            }
                        }
                    };
        } else {
            input = wrapAndSeek(handle.openInputStream(), offset);
        }

        return new StateChangeFormat().read(input);
    }

    private DataInputViewStreamWrapper wrapAndSeek(InputStream stream, long offset)
            throws IOException {
        DataInputViewStreamWrapper wrappedStream = wrap(stream);
        if (offset != 0) {
            LOG.debug("seek to {}", offset);
            wrappedStream.skipBytesToRead((int) offset);
        }
        return wrappedStream;
    }

    private void downloadFile(FileStateHandle handle, FileCache fileCache) {
        try {
            IOUtils.copyBytes(
                    wrap(handle.openInputStream()), fileCache.getOutputStreamForSaveCacheData());
            LOG.debug(
                    "download and decompress dstl file : {} to local cache file : {}",
                    handle.getFilePath(),
                    fileCache.getFilePath());

        } catch (IOException e) {
            fileCache.setSaveCacheDataException(e);
        }
    }

    private void cleanFileCache(Path dfsPath, FileCache fileCache) {
        if (fileCache.getRefCount() == 0) {
            LOG.debug("clean local cache file : {}", fileCache.getFilePath());
            cache.remove(dfsPath);
            fileCache.discard();
        }
    }

    static class FileCache {

        private final File cacheDir;
        private final AtomicLong writeInBytes;
        private final AtomicBoolean writeComplete;
        private final AtomicInteger refCount;
        private final CountDownLatch readLatch;

        private volatile File file;
        private volatile FileOutputStream fo;
        private volatile Exception saveCacheDataException;

        FileCache(File cacheDir) {
            this.cacheDir = cacheDir;
            this.writeInBytes = new AtomicLong(0);
            this.writeComplete = new AtomicBoolean(false);
            this.refCount = new AtomicInteger(0);
            this.readLatch = new CountDownLatch(1);
        }

        String getFilePath() {
            return this.file.getAbsolutePath();
        }

        OutputStream getOutputStreamForSaveCacheData() throws IOException {
            synchronized (this) {
                if (fo == null) {
                    file = File.createTempFile(CACHE_FILE_PREFIX, null, cacheDir);
                    fo = new FileOutputStream(file);
                    readLatch.countDown();
                } else {
                    throw new IllegalStateException("only can get OutputStream once !");
                }
            }

            return new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    fo.write(b);
                    writeInBytes.incrementAndGet();
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    fo.write(b, off, len);
                    writeInBytes.addAndGet(len);
                }

                @Override
                public void close() throws IOException {
                    fo.close();
                    writeComplete.set(true);
                }
            };
        }

        void setSaveCacheDataException(Exception e) {
            this.saveCacheDataException = e;
        }

        int getRefCount() {
            return refCount.get();
        }

        private void handoverException() throws IOException {
            if (saveCacheDataException != null) {
                throw new IOException(
                        "there is a exception when save data to cache file : ",
                        saveCacheDataException);
            }
        }

        FileInputStream open() throws IOException {
            return open0();
        }

        FileInputStream openAndSeek(long offset) throws IOException {
            FileInputStream fin = open0();
            if (offset != 0) {
                LOG.debug("seek to {}", offset);
                fin.getChannel().position(offset);
            }
            return fin;
        }

        private FileInputStream open0() throws IOException {
            try {
                readLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            refCount.incrementAndGet();

            return new FileInputStream(file) {
                private final String id = UUID.randomUUID().toString();
                private final AtomicBoolean closed = new AtomicBoolean(false);

                @Override
                public int read() throws IOException {
                    handoverException();
                    if (writeComplete.get()) {
                        return super.read();
                    } else {
                        int data = super.read();
                        if (data != -1) {
                            return data;
                        } else {
                            waitWrite();
                            return read();
                        }
                    }
                }

                @Override
                public int read(byte[] b) throws IOException {
                    handoverException();
                    if (writeComplete.get()) {
                        return super.read(b);
                    } else {
                        int count = super.read(b);
                        if (count == -1) {
                            return count;
                        } else {
                            waitWrite();
                            return read(b);
                        }
                    }
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    handoverException();
                    if (writeComplete.get()) {
                        return super.read(b, off, len);
                    } else {
                        int count = super.read(b, off, len);
                        if (count != -1) {
                            return count;
                        } else {
                            waitWrite();
                            return read(b, off, len);
                        }
                    }
                }

                @Override
                public long skip(long n) throws IOException {
                    handoverException();
                    if (writeComplete.get()) {
                        return super.skip(n);
                    } else {
                        long skips = super.skip(n);
                        if (skips == n) {
                            return skips;
                        } else {
                            waitWrite();
                            return skip(n - skips);
                        }
                    }
                }

                @Override
                public int available() throws IOException {
                    handoverException();
                    if (writeComplete.get()) {
                        return super.available();
                    } else {
                        int count = super.available();
                        if (count != 0) {
                            return count;
                        } else {
                            return 1;
                        }
                    }
                }

                private void waitWrite() {
                    long writeInBytes0 = writeInBytes.get();
                    while (writeInBytes0 == writeInBytes.get() && !writeComplete.get()) {
                        try {
                            synchronized (fo) {
                                fo.wait(10);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }

                @Override
                public void close() throws IOException {
                    if (!closed.getAndSet(true)) {
                        super.close();
                        refCount.decrementAndGet();
                    }
                }
            };
        }

        void discard() {
            if (file != null) {
                try {
                    Files.delete(file.toPath());
                } catch (IOException e) {
                    LOG.warn("delete local cache file {} failed.", getFilePath());
                }
            }
        }
    }
}
