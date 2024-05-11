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

package org.apache.flink.streaming.examples.regularjoin;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.rocksdb.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Regular join example job. */
public class RegularJoinJob {
    private static final Logger LOG = LoggerFactory.getLogger(RegularJoinJob.class);

    public static final ConfigOption<String> JOB_NAME =
            ConfigOptions.key("jobName")
                    .stringType()
                    .defaultValue("Undefined-Name-Job")
                    .withDescription("Job name");

    public static final ConfigOption<JoinImpl> JOIN_IMPL =
            ConfigOptions.key("joinImpl")
                    .enumType(JoinImpl.class)
                    .defaultValue(JoinImpl.ASYNC_SIMPLE_JOIN)
                    .withDescription("Type of join implementation.");

    // Flat map options ===============================================

    public static final ConfigOption<Integer> FLAT_MAP_PARALLELISM =
            ConfigOptions.key("flatMapParallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The parallelism of Flat Map operator.");

    //  checkpoint options ========================================

    public static final ConfigOption<Long> CHECKPOINT_INTERVAL =
            ConfigOptions.key("checkpointInterval")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Checkpoint interval in milliseconds, and default is Long.MAX_VALUE which"
                                    + "means checkpoint is disable.");

    public static final ConfigOption<String> CHECKPOINT_PATH =
            ConfigOptions.key("checkpointPath")
                    .stringType()
                    .defaultValue("file:///tmp/flink/checkpoint")
                    .withDescription("Checkpoint path");

    // state backend options ======================================

    public static final ConfigOption<String> STATE_BACKEND =
            ConfigOptions.key("stateBackend")
                    .stringType()
                    .defaultValue("rocksdb")
                    .withDescription(
                            "Type of state backend, support memory, fs, gemini, niagara, rocksdb");

    public static final ConfigOption<String> ROCKSDB_COMPRESS =
            ConfigOptions.key("rocksdbCompress")
                    .stringType()
                    .defaultValue(CompressionType.SNAPPY_COMPRESSION.name())
                    .withDescription("Compression type for rocksdb.");

    public static final ConfigOption<Boolean> ROCKSDB_STATS =
            ConfigOptions.key("rocksdbStats").booleanType().defaultValue(false);

    public static final ConfigOption<Boolean> SHARING_GROUP =
            ConfigOptions.key("sharingGroup")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to enable sharing group");

    public enum StateBackendType {
        MEMORY,
        FS,
        GEMINI,
        ROCKSDB
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        Configuration configuration = params.getConfiguration();

        env.getConfig().setGlobalJobParameters(configuration);
        env.disableOperatorChaining();

        String group1 = "default1";
        String group2 = "default2";
        String group3 = "default3";
        if (configuration.get(SHARING_GROUP)) {
            group1 = group2 = group3 = "default";
        }

        int flatMapParallelism = configuration.get(FLAT_MAP_PARALLELISM);

        DataStream<Tuple2<String, Long>> source1 =
                env.addSource(new WordSource(configuration))
                        .name("left-stream")
                        .setParallelism(flatMapParallelism);

        DataStream<Tuple2<String, Long>> source2 =
                env.addSource(new WordSource(configuration))
                        .name("right-stream")
                        .setParallelism(flatMapParallelism);

        SimpleJoin<String, Tuple2<String, Long>, Tuple2<String, Long>, Tuple3<String, Long, Long>>
                simpleJoin;

        switch (configuration.get(JOIN_IMPL)) {
            case SYNC_SIMPLE_JOIN:
                LOG.info("use sync simple join.");
                simpleJoin = SimpleJoins.syncSimpleJoin();
                break;
            case ASYNC_SIMPLE_JOIN:
                LOG.info("use async simple join.");
                simpleJoin = SimpleJoins.asyncSimpleJoin();
                break;
            default:
                throw new IllegalStateException("Not supported: " + configuration.get(JOIN_IMPL));
        }

        TupleTypeInfo<Tuple3<String, Long, Long>> resultInfo =
                new TupleTypeInfo<>(
                        TypeInformation.of(String.class),
                        TypeInformation.of(Long.class),
                        TypeInformation.of(Long.class));

        TypeInformation<Tuple2<String, Long>> recordInfo =
                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {});

        DataStream<Tuple3<String, Long, Long>> result =
                simpleJoin
                        .join(
                                source1,
                                recordInfo.createSerializer(new SerializerConfigImpl()),
                                l -> l.f0,
                                source2,
                                recordInfo.createSerializer(new SerializerConfigImpl()),
                                r -> r.f0,
                                (l, r) -> true,
                                (l, r) -> new Tuple3<>(l.f0, l.f1, r.f1))
                        .name("regular-join")
                        .returns(resultInfo)
                        .slotSharingGroup(group2);

        result.addSink(new JustCountSink<>())
                .name("just-count-sink")
                .setParallelism(flatMapParallelism)
                .slotSharingGroup(group3);

        env.execute(configuration.get(JOB_NAME));
    }

    public static class JustCountSink<T> extends RichSinkFunction<T> {
        private long count = 0;

        private static final long serialVersionUID = 1L;

        public JustCountSink() {}

        @Override
        public void invoke(T value) {
            count++;
            if (count % 1000000 == 0) {
                LOG.info("sink {} records.", count);
            }
        }

        @Override
        public void open(Configuration config) {}
    }

    public enum JoinImpl {
        SYNC_SIMPLE_JOIN,
        ASYNC_SIMPLE_JOIN,
        ;
    }
}
