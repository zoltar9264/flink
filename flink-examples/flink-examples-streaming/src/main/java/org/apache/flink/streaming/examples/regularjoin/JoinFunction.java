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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.IOException;

class JoinFunction<IN_LEFT, IN_RIGHT, OUT> extends AbstractJoinFunction<IN_LEFT, IN_RIGHT, OUT> {

    private transient ValueState<IN_LEFT> leftRecord;
    private transient ValueState<IN_RIGHT> rightRecord;

    public JoinFunction(
            TypeSerializer<IN_LEFT> leftSerializer,
            TypeSerializer<IN_RIGHT> rightSerializer,
            SimpleJoin.JoinCondition<IN_LEFT, IN_RIGHT> joinCondition,
            SimpleJoin.ProcessJoinFunction<IN_LEFT, IN_RIGHT, OUT> processFunction) {
        super(leftSerializer, rightSerializer, joinCondition, processFunction);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.leftRecord =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("LEFT_RECORD", leftSerializer));

        this.rightRecord =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("RIGHT_RECORD", rightSerializer));
    }

    @Override
    public void flatMap(AbstractSimpleJoin.JoinRecord<IN_LEFT, IN_RIGHT> record, Collector<OUT> out)
            throws IOException {
        if (record.isLeft()) {
            leftRecord.update(record.getLeft());
            output(record.getLeft(), rightRecord.value(), out);
        } else {
            rightRecord.update(record.getRight());
            output(leftRecord.value(), record.getRight(), out);
        }
    }
}
