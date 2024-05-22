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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * A context that preserves the necessary variables required by each operation, all operations for
 * one record will share the same element context.
 *
 * <p>Reference counting mechanism, please refer to {@link ContextStateFutureImpl}.
 *
 * @param <K> The type of the key inside the record.
 */
public class RecordContext<K> extends ReferenceCounted<RecordContext.DisposerRunner> {
    private static final Logger LOG = LoggerFactory.getLogger(RecordContext.class);

    /** The empty record for timer and non-record input usage. */
    static final Object EMPTY_RECORD = new Object();

    /** The record to be processed. */
    private final Object record;

    /** The key inside the record. */
    private final K key;

    /** Whether this Record(Context) has occupied the corresponding key. */
    private volatile boolean keyOccupied;

    /**
     * The disposer for disposing this context. This should be invoked in {@link
     * #referenceCountReachedZero}, which may be called once the ref count reaches zero in any
     * thread.
     */
    private final Consumer<RecordContext<K>> disposer;

    /** The keyGroup to which key belongs. */
    private final int keyGroup;

    /**
     * The extra context info which is used to hold customized data defined by state backend. The
     * state backend can use this field to cache some data that can be used multiple times in
     * different stages of asynchronous state execution.
     */
    private @Nullable volatile Object extra;

    /** The epoch of this context. */
    private final Epoch epoch;

    private final String id;

    private final ArrayList<StackTraceElement[]> stackTraces;

    private static volatile boolean printed = true;

    public RecordContext(
            Object record, K key, Consumer<RecordContext<K>> disposer, int keyGroup, Epoch epoch) {
        super(0);
        this.record = record;
        this.key = key;
        this.keyOccupied = false;
        this.disposer = disposer;
        this.keyGroup = keyGroup;
        this.epoch = epoch;
        this.id = UUID.randomUUID().toString();
        this.stackTraces = new ArrayList<>(15);
    }

    public Object getRecord() {
        return record;
    }

    public K getKey() {
        return this.key;
    }

    /** Check if this context has occupied the key. */
    boolean isKeyOccupied() {
        return keyOccupied;
    }

    /** Set the flag that marks this context has occupied the corresponding key. */
    void setKeyOccupied() {
        keyOccupied = true;
    }

    @Override
    public int retain() {
        int ret = super.retain();
        synchronized (stackTraces) {
            stackTraces.add(Thread.currentThread().getStackTrace());
        }
        return ret;
    }

    @Override
    public int release(@Nullable DisposerRunner disposerRunner) {
        int ret = super.release(disposerRunner);
        synchronized (stackTraces) {
            stackTraces.add(Thread.currentThread().getStackTrace());
        }
        return ret;
    }

    @Override
    protected void referenceCountReachedZero(@Nullable DisposerRunner disposerRunner) {
        if (keyOccupied) {
            keyOccupied = false;
            if (disposerRunner != null) {
                disposerRunner.runDisposer(() -> disposer.accept(this));
            } else {
                disposer.accept(this);
            }
        }
    }

    public int getKeyGroup() {
        return keyGroup;
    }

    public void setExtra(Object extra) {
        this.extra = extra;
    }

    public Object getExtra() {
        return extra;
    }

    public Epoch getEpoch() {
        return epoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(record, key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordContext<?> that = (RecordContext<?>) o;
        if (!Objects.equals(record, that.record)) {
            return false;
        }
        if (!Objects.equals(keyGroup, that.keyGroup)) {
            return false;
        }
        if (!Objects.equals(epoch, that.epoch)) {
            return false;
        }
        return Objects.equals(key, that.key);
    }

    @Override
    public String toString() {
        return "RecordContext{"
                + "record="
                + record
                + ", key="
                + key
                + ", occupied="
                + keyOccupied
                + ", ref="
                + getReferenceCount()
                + ", epoch="
                + epoch.id
                + "}";
    }

    public interface DisposerRunner {
        void runDisposer(Runnable task);
    }

    @Override
    protected void finalize() throws Throwable {
        if (getReferenceCount() > 0) {
            LOG.error(spellInfo());
        } else if (!printed) {
            synchronized (RecordContext.class) {
                if (!printed) {
                    printed = true;
                    LOG.info(spellInfo());
                }
            }
        }
    }

    private String spellInfo() {
        StringBuilder sb =
                new StringBuilder("recordContext:")
                        .append(id)
                        .append(" finalized with refcount ")
                        .append(getReferenceCount())
                        .append(" :\n");
        int i = 0;
        for (StackTraceElement[] stackTrace : stackTraces) {
            sb.append("recordContext stackTrace ").append(i++).append(" :\n");
            for (StackTraceElement stackTraceElement : stackTrace) {
                sb.append("        ").append(stackTraceElement).append("\n");
            }
        }
        sb.append("recordContext:").append(id).append(" print finished.");
        return sb.toString();
    }
}
