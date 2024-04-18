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

import org.apache.flink.api.common.ExecutionConfig;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Regular join example job. */
public class RegularJoinJob {
    private static final Logger LOG = LoggerFactory.getLogger(RegularJoinJob.class);

    private static final String DEFAULT_JOB_NAME = "regular-join";

    // Job common options start ===============================================

    public static final ConfigOption<String> JOB_NAME =
            ConfigOptions.key("jobName")
                    .stringType()
                    .defaultValue("Undefined-Name-Job")
                    .withDescription("Job name");

    public static final ConfigOption<Boolean> SHARING_GROUP =
            ConfigOptions.key("sharingGroup")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to enable sharing group");

    // RegularJoinJob common options start ===============================================

    public static final ConfigOption<JoinImpl> JOIN_IMPL =
            ConfigOptions.key("joinImpl")
                    .enumType(JoinImpl.class)
                    .defaultValue(JoinImpl.SYNC_SIMPLE_JOIN)
                    .withDescription("Type of join implementation.");

    // All options end  ===============================================

    public static void main(String[] args) throws Exception {

        Configuration conf = ParameterTool.fromArgs(args).getConfiguration();

        if (!conf.contains(JOB_NAME)) {
            conf.set(JOB_NAME, DEFAULT_JOB_NAME);
        }

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.getConfig().setGlobalJobParameters(conf);
        env.disableOperatorChaining();

        String group1 = "default1";
        String group2 = "default2";
        String group3 = "default3";
        if (conf.get(SHARING_GROUP)) {
            group1 = group2 = group3 = "default";
        }

        DataStream<Tuple2<String, Long>> source1 =
                env.addSource(new WordSource(conf)).name("left-stream").slotSharingGroup(group1);

        DataStream<Tuple2<String, Long>> source2 =
                env.addSource(new WordSource(conf)).name("right-stream").slotSharingGroup(group1);

        SimpleJoin<String, Tuple2<String, Long>, Tuple2<String, Long>, Tuple3<String, Long, Long>>
                simpleJoin;

        switch (conf.get(JOIN_IMPL)) {
            case SYNC_SIMPLE_JOIN:
                LOG.info("use sync simple join.");
                simpleJoin = SimpleJoins.syncSimpleJoin();
                break;
            case ASYNC_SIMPLE_JOIN:
                LOG.info("use async simple join.");
                simpleJoin = SimpleJoins.asyncSimpleJoin();
                break;
            default:
                throw new IllegalStateException("Not supported: " + conf.get(JOIN_IMPL));
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
                                recordInfo.createSerializer(new ExecutionConfig()),
                                l -> l.f0,
                                source2,
                                recordInfo.createSerializer(new ExecutionConfig()),
                                r -> r.f0,
                                (l, r) -> true,
                                (l, r) -> new Tuple3<>(l.f0, l.f1, r.f1))
                        .name("regular-join")
                        .returns(resultInfo)
                        .slotSharingGroup(group2);

        result.addSink(new JustCountSink<>()).name("just-count-sink").slotSharingGroup(group3);

        env.execute(conf.get(JOB_NAME));
    }

    public static class JustCountSink<T> extends RichSinkFunction<T> {
        private long count = 0;

        private static final long serialVersionUID = 1L;

        public JustCountSink() {}

        @Override
        public void open(Configuration config) {}

        @Override
        public void invoke(T value, Context context) throws Exception {
            count++;
            if (count % 100_0000 == 0) {
                LOG.info("sink {} records.", count);
            }
        }
    }

    public enum JoinImpl {
        SYNC_SIMPLE_JOIN,
        ASYNC_SIMPLE_JOIN,
        ;
    }
}
