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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.runtime.asyncprocessing.AbstractStateExecutor;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class RocksDBStateExecutor<K> extends AbstractStateExecutor {

    private boolean aecBound = false;

    @Override
    public void bindAsyncExecutionController(AsyncExecutionController<?> asyncExecutionController) {
        if (aecBound) {
            throw new IllegalStateException("AsyncExecutionController already been bound.");
        } else {
            super.bindAsyncExecutionController(asyncExecutionController);
            aecBound = true;
        }
    }

    public AsyncExecutionController<K> getAec() {
        if (!aecBound) {
            throw new IllegalStateException("AsyncExecutionController has not been bound.");
        } else {
            return (AsyncExecutionController<K>) asyncExecutionController;
        }
    }

    @Override
    public CompletableFuture<Boolean> executeBatchRequests(
            Iterable<StateRequest<?, ?, ?>> processingRequests) {
        for (StateRequest<?, ?, ?> request : processingRequests) {
            // execute state request one by one
            processRequest((StateRequest<K, ?, ?>) request);
        }

        return CompletableFuture.completedFuture(Boolean.TRUE);
    }

    private <IN, OUT> void processRequest(StateRequest<K, IN, OUT> request) {
        State state = request.getState();
        StateRequestType requestType = request.getRequestType();
        if (state instanceof ValueState) {
            switch (requestType) {
                case CLEAR:
                    processValueStateClear(request);
                    break;
                case VALUE_GET:
                    processValueStateGet(request);
                    break;
                case VALUE_UPDATE:
                    processValueStateUpdate(request);
                    break;
                default:
                    throw new IllegalStateException(
                            String.format("%s not support %s", state.getClass(), requestType));
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("%s not supported yet.", request.getState().getClass()));
        }
    }

    private <IN, OUT> void processValueStateClear(StateRequest<K, IN, OUT> request) {
        RocksDBValueStateV2<K, ?> valueStateV2 = (RocksDBValueStateV2<K, ?>) request.getState();
        InternalStateFuture<OUT> stateFuture = request.getFuture();
        RecordContext<K> recordContext = request.getRecordContext();

        valueStateV2.setCurrentKey(recordContext.getKey());

        valueStateV2.getValueState().clear();
        stateFuture.complete(null);
    }

    private <IN, OUT> void processValueStateGet(StateRequest<K, IN, OUT> request) {
        RocksDBValueStateV2<K, OUT> valueStateV2 = (RocksDBValueStateV2<K, OUT>) request.getState();
        InternalStateFuture<OUT> stateFuture = request.getFuture();
        RecordContext<K> recordContext = request.getRecordContext();

        valueStateV2.setCurrentKey(recordContext.getKey());

        try {
            stateFuture.complete(valueStateV2.getValueState().value());
        } catch (IOException e) {
            throw new FlinkRuntimeException("process value state process failed :", e);
        }
    }

    private <IN, OUT> void processValueStateUpdate(StateRequest<K, IN, OUT> request) {
        RocksDBValueStateV2<K, IN> valueStateV2 = (RocksDBValueStateV2<K, IN>) request.getState();
        InternalStateFuture<OUT> stateFuture = request.getFuture();
        RecordContext<K> recordContext = request.getRecordContext();

        valueStateV2.setCurrentKey(recordContext.getKey());

        try {
            valueStateV2.getValueState().update(request.getPayload());
            stateFuture.complete(null);
        } catch (IOException e) {
            throw new FlinkRuntimeException("process value state process failed :", e);
        }
    }
}
