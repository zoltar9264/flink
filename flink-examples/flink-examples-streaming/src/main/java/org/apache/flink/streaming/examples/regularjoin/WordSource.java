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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

import static java.util.Objects.requireNonNull;

public class WordSource extends RichParallelSourceFunction<Tuple2<String, Long>> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(WordSource.class);

    public static final ConfigOption<Integer> WORD_NUMBER =
            ConfigOptions.key("wordNumber")
                    .intType()
                    .defaultValue(300000000)
                    .withDescription("Number of different words which will influence state size.");

    public static final ConfigOption<Integer> WORD_LENGTH =
            ConfigOptions.key("wordLength")
                    .intType()
                    .defaultValue(16)
                    .withDescription("Length of word which will influence state size.");

    public static final ConfigOption<Integer> WORD_RATE =
            ConfigOptions.key("wordRate")
                    .intType()
                    .defaultValue(1000000)
                    .withDescription("Rate to emit words");

    public static final ConfigOption<Long> WORD_MAX_COUNT =
            ConfigOptions.key("wordMaxCount")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("Total records this source can emit.");

    public static final ConfigOption<WordDistribution> WORD_DISTRIBUTION =
            ConfigOptions.key("wordDistribution")
                    .enumType(WordDistribution.class)
                    .defaultValue(WordDistribution.RANDOM)
                    .withDescription("Distribution of words which will influence data locality.");

    public static final ConfigOption<Double> WORD_GAUSSIAN_SIGMA =
            ConfigOptions.key("wordGaussianSigma")
                    .doubleType()
                    .defaultValue(0.3)
                    .withDescription("Sigma of Gaussian distribution.");

    private final Configuration config;

    private final long maxCount;

    private final int wordLen;

    private final int largest;

    private long baseNumForSubTask;

    private final long rate;

    private final String wordFormat;

    private transient ThrottledIterator<Integer> throttledIterator;

    private transient int emitNumber;

    private transient volatile boolean isRunning;

    public WordSource(Configuration config) {
        this.config = config;

        this.rate = config.get(WORD_RATE);
        this.wordLen = config.get(WORD_LENGTH);
        this.largest = config.get(WORD_NUMBER);
        this.maxCount = config.get(WORD_MAX_COUNT);

        this.wordFormat = "%0" + this.wordLen + "d";
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("create WordSource with configuration: {}", config);

        this.isRunning = true;
        this.emitNumber = 0;
        this.baseNumForSubTask = ((long) getRuntimeContext().getIndexOfThisSubtask()) * largest;

        Iterator<Integer> numberSourceIterator;

        switch (config.get(WORD_DISTRIBUTION)) {
            case RANDOM:
                numberSourceIterator =
                        new RandomNumberSourceIterator(largest, System.currentTimeMillis());
                break;
            case GAUSSIAN:
                numberSourceIterator =
                        new GaussianNumberSourceIterator(
                                config.get(WORD_GAUSSIAN_SIGMA),
                                largest,
                                System.currentTimeMillis());
                break;
            default:
                throw new IllegalStateException(
                        "Unexpected Word Distribution: " + config.get(WORD_DISTRIBUTION));
        }

        this.throttledIterator = new ThrottledIterator<>(numberSourceIterator, rate);
    }

    @Override
    public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
        while (isRunning) {
            if (maxCount < 0) {
                Integer number;
                if (emitNumber < largest) {
                    number = emitNumber++;
                } else {
                    number = throttledIterator.next();
                }
                sourceContext.collect(
                        Tuple2.of(
                                covertToString(baseNumForSubTask + number),
                                System.currentTimeMillis()));
            } else {
                isRunning = false;
                break;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() {
        isRunning = false;
    }

    private String covertToString(long number) {
        return String.format(wordFormat, number);
    }

    // ------------------------------------------------------------------------
    //  Number generator
    // ------------------------------------------------------------------------

    public enum WordDistribution {
        RANDOM,
        GAUSSIAN
    }

    static class RandomNumberSourceIterator implements Iterator<Integer>, Serializable {
        private final int largest;
        private final Random rnd;

        public RandomNumberSourceIterator(int largest, long seed) {
            this.largest = largest;
            this.rnd = new Random(seed);
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Integer next() {
            return rnd.nextInt(largest + 1);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    static class GaussianNumberSourceIterator implements Iterator<Integer>, Serializable {
        private final double sigma;
        private final int largest;
        private final Random rnd;

        private long count;

        public GaussianNumberSourceIterator(double sigma, int largest, long seed) {
            this.sigma = sigma;
            this.largest = largest;
            this.rnd = new Random(seed);

            this.count = 0;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Integer next() {
            double x = (rnd.nextGaussian() * largest * sigma + count++) % largest;
            return (int) (x < 0 ? x + largest : x);
        }
    }

    static class ThrottledIterator<T> implements Iterator<T>, Serializable {

        private static final long serialVersionUID = 1L;

        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Iterator<T> source;

        private final long sleepBatchSize;
        private final long sleepBatchTime;

        private long lastBatchCheckTime;
        private long num;

        public ThrottledIterator(Iterator<T> source, long elementsPerSecond) {
            this.source = requireNonNull(source);

            if (elementsPerSecond >= 100) {
                // how many elements would we emit per 50ms
                this.sleepBatchSize = elementsPerSecond / 20;
                this.sleepBatchTime = 50;
            } else if (elementsPerSecond >= 1) {
                // how long does element take
                this.sleepBatchSize = 1;
                this.sleepBatchTime = 1000 / elementsPerSecond;
            } else {
                throw new IllegalArgumentException(
                        "'elements per second' must be positive and not zero");
            }
        }

        @Override
        public boolean hasNext() {
            return source.hasNext();
        }

        @Override
        public T next() {
            // delay if necessary
            if (lastBatchCheckTime > 0) {
                if (++num >= sleepBatchSize) {
                    num = 0;

                    final long now = System.currentTimeMillis();
                    final long elapsed = now - lastBatchCheckTime;
                    if (elapsed < sleepBatchTime) {
                        try {
                            Thread.sleep(sleepBatchTime - elapsed);
                        } catch (InterruptedException e) {
                            // restore interrupt flag and proceed
                            Thread.currentThread().interrupt();
                        }
                    }
                    lastBatchCheckTime = System.currentTimeMillis();
                }
            } else {
                lastBatchCheckTime = System.currentTimeMillis();
            }

            return source.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
