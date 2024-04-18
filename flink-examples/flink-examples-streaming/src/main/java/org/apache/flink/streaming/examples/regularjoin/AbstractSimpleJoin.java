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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.io.Serializable;

/** An implementation of SimpleJoin which use flat map function. */
public abstract class AbstractSimpleJoin<K, IN_LEFT, IN_RIGHT, OUT>
        implements SimpleJoin<K, IN_LEFT, IN_RIGHT, OUT> {

    @Override
    public SingleOutputStreamOperator<OUT> join(
            DataStream<IN_LEFT> leftStream,
            TypeSerializer<IN_LEFT> leftSerializer,
            KeySelector<IN_LEFT, K> leftKeySelector,
            DataStream<IN_RIGHT> rightStream,
            TypeSerializer<IN_RIGHT> rightSerializer,
            KeySelector<IN_RIGHT, K> rightKeySelector,
            JoinCondition<IN_LEFT, IN_RIGHT> joinCondition,
            ProcessJoinFunction<IN_LEFT, IN_RIGHT, OUT> processFunction) {

        DataStream<JoinRecord<IN_LEFT, IN_RIGHT>> leftJoinStream =
                leftStream.map(new LeftMapFunc()).name("left-map");

        DataStream<JoinRecord<IN_LEFT, IN_RIGHT>> rightJoinStream =
                rightStream.map(new RightMapFunc()).name("right-map");

        return leftJoinStream
                .union(rightJoinStream)
                .keyBy(
                        e ->
                                e.isLeft()
                                        ? leftKeySelector.getKey(e.getLeft())
                                        : rightKeySelector.getKey(e.getRight()))
                .flatMap(
                        getJoinFunction(
                                leftSerializer, rightSerializer, joinCondition, processFunction));
    }

    abstract FlatMapFunction<JoinRecord<IN_LEFT, IN_RIGHT>, OUT> getJoinFunction(
            TypeSerializer<IN_LEFT> leftSerializer,
            TypeSerializer<IN_RIGHT> rightSerializer,
            JoinCondition<IN_LEFT, IN_RIGHT> joinCondition,
            ProcessJoinFunction<IN_LEFT, IN_RIGHT, OUT> processFunction);

    public static class JoinRecord<IN_LEFT, IN_RIGHT> implements Serializable {
        private final IN_LEFT left;
        private final IN_RIGHT right;
        private final boolean isLeft;

        private JoinRecord(IN_LEFT left, IN_RIGHT right) {
            this.left = left;
            this.right = right;

            if (left != null && right == null) {
                this.isLeft = true;
            } else if (left == null && right != null) {
                this.isLeft = false;
            } else {
                throw new IllegalArgumentException("left and right must and only one is null.");
            }
        }

        public IN_LEFT getLeft() {
            return left;
        }

        public IN_RIGHT getRight() {
            return right;
        }

        public boolean isLeft() {
            return isLeft;
        }
    }

    public class LeftMapFunc implements MapFunction<IN_LEFT, JoinRecord<IN_LEFT, IN_RIGHT>> {

        @Override
        public JoinRecord<IN_LEFT, IN_RIGHT> map(IN_LEFT value) throws Exception {
            return new JoinRecord<>(value, null);
        }
    }

    public class RightMapFunc implements MapFunction<IN_RIGHT, JoinRecord<IN_LEFT, IN_RIGHT>> {

        @Override
        public JoinRecord<IN_LEFT, IN_RIGHT> map(IN_RIGHT value) throws Exception {
            return new JoinRecord<>(null, value);
        }
    }
}
