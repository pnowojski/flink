/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.lifecycle.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.lifecycle.TestJobWithDescription;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandQueue;
import org.apache.flink.runtime.operators.lifecycle.event.DataSentEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.MultipleConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.util.function.TriFunction;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart;
import static org.apache.flink.configuration.JobManagerOptions.EXECUTION_FAILOVER_STRATEGY;

/** Helper to build {@link TestJobWithDescription}. */
public class TestJobBuilders {

    /** {@link TestJobWithDescription} builder. */
    public interface TestingGraphBuilder
            extends TriFunction<
                    SharedObjects,
                    Consumer<Configuration>,
                    Consumer<StreamExecutionEnvironment>,
                    TestJobWithDescription> {}

    private TestJobBuilders() {}

    public static final TestingGraphBuilder SIMPLE_GRAPH_BUILDER =
            new TestingGraphBuilder() {
                @Override
                public TestJobWithDescription apply(
                        SharedObjects shared,
                        Consumer<Configuration> confConsumer,
                        Consumer<StreamExecutionEnvironment> envConsumer) {

                    TestEventQueue eventQueue = TestEventQueue.createShared(shared);
                    TestCommandQueue commandQueue = TestCommandQueue.createShared(shared);
                    StreamExecutionEnvironment env = prepareEnv(confConsumer, envConsumer);

                    // using hashes so that operators emit identifiable events
                    String unitedSourceLeft = OP_ID_HASH_PREFIX + "1";
                    String mapForward = OP_ID_HASH_PREFIX + "5";

                    DataStream<TestEvent> src =
                            env.addSource(
                                            new TestEventSource(
                                                    unitedSourceLeft, eventQueue, commandQueue))
                                    .setUidHash(unitedSourceLeft)
                                    .assignTimestampsAndWatermarks(createWmAssigner());

                    SingleOutputStreamOperator<TestEvent> forwardTransform =
                            src.transform(
                                            "transform-1-forward",
                                            TypeInformation.of(TestEvent.class),
                                            new OneInputTestStreamOperatorFactory(
                                                    mapForward, eventQueue))
                                    .setUidHash(mapForward);

                    forwardTransform.addSink(new DiscardingSink<>());

                    Map<String, Integer> operatorsNumberOfInputs = new HashMap<>();
                    operatorsNumberOfInputs.put(mapForward, 1);

                    return new TestJobWithDescription(
                            env.getStreamGraph().getJobGraph(),
                            new HashSet<>(singletonList(mapForward)),
                            new HashSet<>(asList(unitedSourceLeft, mapForward)),
                            operatorsNumberOfInputs,
                            eventQueue,
                            commandQueue);
                }

                @Override
                public String toString() {
                    return "simple graph";
                }
            };

    public static final TestingGraphBuilder COMPLEX_GRAPH_BUILDER =
            new TestingGraphBuilder() {
                @Override
                public TestJobWithDescription apply(
                        SharedObjects shared,
                        Consumer<Configuration> confConsumer,
                        Consumer<StreamExecutionEnvironment> envConsumer) {

                    TestEventQueue eventQueue = TestEventQueue.createShared(shared);
                    TestCommandQueue commandQueue = TestCommandQueue.createShared(shared);

                    StreamExecutionEnvironment env = prepareEnv(confConsumer, envConsumer);

                    // using hashes so that operators emit identifiable events
                    String unitedSourceLeft = OP_ID_HASH_PREFIX + "1";
                    String unitedSourceRight = OP_ID_HASH_PREFIX + "2";
                    String connectedSource = OP_ID_HASH_PREFIX + "3";
                    String multiSource = OP_ID_HASH_PREFIX + "4";
                    String mapForward = OP_ID_HASH_PREFIX + "5";
                    String mapKeyed = OP_ID_HASH_PREFIX + "6";
                    String mapTwoInput = OP_ID_HASH_PREFIX + "7";
                    String multipleInput = OP_ID_HASH_PREFIX + "8";

                    // todo: FLIP-27 sources
                    // todo: chain sources
                    DataStream<TestEvent> unitedSources =
                            env.addSource(
                                            new TestEventSource(
                                                    unitedSourceLeft, eventQueue, commandQueue))
                                    .setUidHash(unitedSourceLeft)
                                    .assignTimestampsAndWatermarks(createWmAssigner())
                                    .union(
                                            env.addSource(
                                                            new TestEventSource(
                                                                    unitedSourceRight,
                                                                    eventQueue,
                                                                    commandQueue))
                                                    .setUidHash(unitedSourceRight)
                                                    .assignTimestampsAndWatermarks(
                                                            createWmAssigner()));
                    SingleOutputStreamOperator<TestEvent> sideSource =
                            env.addSource(
                                            new TestEventSource(
                                                    multiSource, eventQueue, commandQueue))
                                    .setUidHash(multiSource)
                                    .assignTimestampsAndWatermarks(createWmAssigner());

                    DataStream<?>[] inputs = new DataStream[] {unitedSources, sideSource};
                    final MultipleInputTransformation<TestEvent> multipleInputsTransform =
                            new MultipleInputTransformation<>(
                                    "MultipleInputOperator",
                                    new MultiInputTestOperatorFactory(
                                            inputs.length, eventQueue, multipleInput),
                                    TypeInformation.of(TestEvent.class),
                                    env.getParallelism());
                    for (DataStream<?> input : inputs) {
                        multipleInputsTransform.addInput(input.getTransformation());
                    }
                    multipleInputsTransform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);
                    env.addOperator(multipleInputsTransform);

                    SingleOutputStreamOperator<TestEvent> multipleSources =
                            new MultipleConnectedStreams(env)
                                    .transform(multipleInputsTransform)
                                    .setUidHash(multiSource);

                    SingleOutputStreamOperator<TestEvent> forwardTransform =
                            multipleSources
                                    .startNewChain()
                                    .transform(
                                            "transform-1-forward",
                                            TypeInformation.of(TestEvent.class),
                                            new OneInputTestStreamOperatorFactory(
                                                    mapForward, eventQueue))
                                    .setUidHash(mapForward);

                    SingleOutputStreamOperator<TestEvent> keyedTransform =
                            forwardTransform
                                    .startNewChain()
                                    .keyBy(e -> e)
                                    .transform(
                                            "transform-2-keyed",
                                            TypeInformation.of(TestEvent.class),
                                            new OneInputTestStreamOperatorFactory(
                                                    mapKeyed, eventQueue))
                                    .setUidHash(mapKeyed);

                    SingleOutputStreamOperator<TestEvent> twoInputTransform =
                            keyedTransform
                                    .startNewChain()
                                    .connect(
                                            env.addSource(
                                                            new TestEventSource(
                                                                    connectedSource,
                                                                    eventQueue,
                                                                    commandQueue))
                                                    .setUidHash(connectedSource))
                                    .transform(
                                            "transform-3-two-input",
                                            TypeInformation.of(TestEvent.class),
                                            new TwoInputTestStreamOperator(mapTwoInput, eventQueue))
                                    .setUidHash(mapTwoInput);

                    twoInputTransform.addSink(new DiscardingSink<>());

                    Map<String, Integer> operatorsNumberOfInputs = new HashMap<>();
                    operatorsNumberOfInputs.put(mapForward, 1);
                    operatorsNumberOfInputs.put(mapKeyed, 1);
                    operatorsNumberOfInputs.put(mapTwoInput, 2);
                    operatorsNumberOfInputs.put(multipleInput, 2);

                    return new TestJobWithDescription(
                            env.getStreamGraph().getJobGraph(),
                            new HashSet<>(asList(mapForward, mapKeyed, mapTwoInput, multipleInput)),
                            new HashSet<>(
                                    asList(
                                            unitedSourceLeft,
                                            unitedSourceRight,
                                            connectedSource,
                                            mapForward,
                                            mapKeyed,
                                            mapTwoInput,
                                            multipleInput)),
                            operatorsNumberOfInputs,
                            eventQueue,
                            commandQueue);
                }

                @Override
                public String toString() {
                    return "complex graph";
                }
            };

    private static StreamExecutionEnvironment prepareEnv(
            Consumer<Configuration> confConsumer,
            Consumer<StreamExecutionEnvironment> envConsumer) {
        Configuration configuration = new Configuration();
        configuration.set(EXECUTION_FAILOVER_STRATEGY, "full");
        confConsumer.accept(configuration);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(4);
        env.setRestartStrategy(noRestart());
        env.enableCheckpointing(200); // shouldn't matter
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().setAutoWatermarkInterval(50);
        envConsumer.accept(env);
        return env;
    }

    private static final String OP_ID_HASH_PREFIX = "0000000000000000000000000000000";

    @SuppressWarnings("deprecation")
    private static AssignerWithPeriodicWatermarks<TestEvent> createWmAssigner() {
        return new AssignerWithPeriodicWatermarks<TestEvent>() {
            private Watermark watermark;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return watermark;
            }

            @Override
            public long extractTimestamp(TestEvent element, long recordTimestamp) {
                if (element instanceof DataSentEvent) {
                    this.watermark = new Watermark(((DataSentEvent) element).seq);
                    return ((DataSentEvent) element).seq;
                }
                return recordTimestamp;
            }
        };
    }
}
