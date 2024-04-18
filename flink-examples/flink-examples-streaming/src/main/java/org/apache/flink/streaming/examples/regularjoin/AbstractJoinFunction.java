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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractJoinFunction<IN_LEFT, IN_RIGHT, OUT>
        extends RichFlatMapFunction<AbstractSimpleJoin.JoinRecord<IN_LEFT, IN_RIGHT>, OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJoinFunction.class);

    protected final TypeSerializer<IN_LEFT> leftSerializer;
    protected final TypeSerializer<IN_RIGHT> rightSerializer;
    private final SimpleJoin.JoinCondition<IN_LEFT, IN_RIGHT> joinCondition;
    private final SimpleJoin.ProcessJoinFunction<IN_LEFT, IN_RIGHT, OUT> processFunction;

    private transient long matchCount = 0;
    private transient long misMatchCount = 0;

    public AbstractJoinFunction(
            TypeSerializer<IN_LEFT> leftSerializer,
            TypeSerializer<IN_RIGHT> rightSerializer,
            SimpleJoin.JoinCondition<IN_LEFT, IN_RIGHT> joinCondition,
            SimpleJoin.ProcessJoinFunction<IN_LEFT, IN_RIGHT, OUT> processFunction) {
        this.leftSerializer = leftSerializer;
        this.rightSerializer = rightSerializer;
        this.joinCondition = joinCondition;
        this.processFunction = processFunction;
    }

    protected void output(IN_LEFT left, IN_RIGHT right, Collector<OUT> out) {
        if (left != null && right != null && joinCondition.isMatch(left, right)) {
            out.collect(processFunction.process(left, right));
            matchCount++;
        } else {
            misMatchCount++;
        }
        if ((matchCount + misMatchCount) % 100_0000 == 0) {
            LOG.info("join match ratio : {}", matchCount * 1.0 / (matchCount + misMatchCount));
        }
    }
}
