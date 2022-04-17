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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleStreamHandleReader;
import org.apache.flink.util.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/** StateChangeIterator default implementation. */
class StateChangeIteratorImpl
        implements StateChangelogHandleStreamHandleReader.StateChangeIterator {
    private static final Logger LOG = LoggerFactory.getLogger(StateChangeIteratorImpl.class);

    @Override
    public CloseableIterator<StateChange> read(StreamStateHandle handle, long offset)
            throws IOException {
        FSDataInputStream stream = handle.openInputStream();
        DataInputViewStreamWrapper input = wrap(stream);
        if (offset != 0) {
            LOG.debug("seek to {}", offset);
            input.skipBytesToRead((int) offset);
        }

        return new StateChangeFormat().read(input);
    }

    protected DataInputViewStreamWrapper wrap(InputStream stream) throws IOException {
        stream = new BufferedInputStream(stream);
        boolean compressed = stream.read() == 1;
        return new DataInputViewStreamWrapper(
                compressed
                        ? SnappyStreamCompressionDecorator.INSTANCE.decorateWithCompression(stream)
                        : stream);
    }
}
