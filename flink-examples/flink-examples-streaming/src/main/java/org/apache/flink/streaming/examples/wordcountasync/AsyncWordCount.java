/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.wordcountasync;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;

import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.FLAT_MAP_PARALLELISM;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.JOB_NAME;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.SHARING_GROUP;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.STATE_MODE;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.TTL;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.WORD_LENGTH;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.WORD_NUMBER;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.WORD_RATE;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.configureCheckpoint;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.getConfiguration;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.setStateBackend;

/** Benchmark mainly used for {@link ValueState} and only support 1 parallelism. */
public class AsyncWordCount {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        Configuration configuration = getConfiguration(params);

        env.getConfig().setGlobalJobParameters(configuration);
        env.disableOperatorChaining();

        String jobName = configuration.get(JOB_NAME);

        configureCheckpoint(env, configuration);

        String group1 = "default1";
        String group2 = "default2";
        String group3 = "default3";
        if (configuration.get(SHARING_GROUP)) {
            group1 = group2 = group3 = "default";
        }

        setStateBackend(env, configuration);

        // configure source
        int wordNumber = configuration.get(WORD_NUMBER);
        int wordLength = configuration.get(WORD_LENGTH);
        int wordRate = configuration.get(WORD_RATE);
        int flatMapParallelism = configuration.get(FLAT_MAP_PARALLELISM);

        DataStream<Tuple2<String, Long>> source =
                WordSource.getSource(env, wordRate, wordNumber, wordLength)
                        .setParallelism(flatMapParallelism)
                        .slotSharingGroup(group1);

        // configure ttl
        long ttl = configuration.get(TTL).toMillis();

        FlatMapFunction<Tuple2<String, Long>, Long> flatMapFunction =
                getFlatMapFunction(configuration, ttl);
        DataStream<Long> mapper =
                source.keyBy(r -> r.f0)
                        .flatMap(flatMapFunction)
                        .setParallelism(flatMapParallelism)
                        .slotSharingGroup(group2);

        mapper.addSink(new BlackHoleSink<>())
                .setParallelism(flatMapParallelism)
                .slotSharingGroup(group3);

        env.execute(jobName);
    }

    private static FlatMapFunction<Tuple2<String, Long>, Long> getFlatMapFunction(
            Configuration configuration, long ttl) {
        JobConfig.StateMode stateMode =
                JobConfig.StateMode.valueOf(configuration.get(STATE_MODE).toUpperCase());

        switch (stateMode) {
            case WRITE:
                throw new FlinkRuntimeException("not support WRITE mode");
            case READ:
                throw new FlinkRuntimeException("not support READ mode");
            case MIXED:
            default:
                return new MixedFlatMapper();
        }
    }

    public static class CustomTypeSerializer extends TypeSerializerSingleton<Object> {

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public Object createInstance() {
            return 0;
        }

        @Override
        public Object copy(Object o) {
            return ((Integer) o).intValue();
        }

        @Override
        public Object copy(Object o, Object t1) {
            return null;
        }

        @Override
        public int getLength() {
            return 4;
        }

        @Override
        public void serialize(Object o, DataOutputView dataOutputView) throws IOException {
            System.out.println("Serializing " + o.toString());
            dataOutputView.writeInt((Integer) o);
        }

        @Override
        public Object deserialize(DataInputView dataInputView) throws IOException {
            int a = dataInputView.readInt();
            System.out.println("Deserializing " + a);
            return a;
        }

        @Override
        public Object deserialize(Object o, DataInputView dataInputView) throws IOException {
            int a = dataInputView.readInt();
            System.out.println("Deserializing " + a);
            return a;
        }

        @Override
        public void copy(DataInputView dataInputView, DataOutputView dataOutputView)
                throws IOException {
            dataOutputView.write(dataInputView, 4);
        }

        @Override
        public TypeSerializerSnapshot<Object> snapshotConfiguration() {
            return new CustomTypeSerializerSnapshot();
        }
    }

    static CustomTypeSerializer INSTANCE = new CustomTypeSerializer();

    public static final class CustomTypeSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<Object> {
        public CustomTypeSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }

    /** Write and read mixed mapper. */
    public static class MixedFlatMapper extends RichFlatMapFunction<Tuple2<String, Long>, Long> {

        private transient ValueState<Integer> wordCounter;

        public MixedFlatMapper() {}

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>(
                            "wc", TypeInformation.of(new TypeHint<Integer>() {}));
            wordCounter = getRuntimeContext().getStateV2(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, Long> in, Collector<Long> out) throws IOException {
            wordCounter
                    .asyncValue()
                    .thenAccept(
                            currentValue -> {
                                if (currentValue != null) {
                                    wordCounter
                                            .asyncUpdate(currentValue + 1)
                                            .thenAccept(empty -> out.collect(currentValue + 1L));
                                } else {
                                    wordCounter.asyncUpdate(1).thenAccept(empty -> out.collect(1L));
                                }
                            });
        }
    }
}
