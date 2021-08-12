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

package org.apache.flink.runtime.operators.lifecycle;

import org.apache.flink.runtime.operators.lifecycle.event.CheckpointCompletedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorStartedEvent;
import org.apache.flink.runtime.operators.lifecycle.graph.TestJobBuilders.TestingGraphBuilder;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.SharedObjects;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import static org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FAIL;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FINISH_SOURCES;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommandQueue.TestCommandRetention.REMOVE_ON_MATCH;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommandQueue.TestCommandRetention.RETAIN_ON_MATCH;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommandQueue.TestCommandTarget.ALL;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommandQueue.TestCommandTarget.forOperatorId;
import static org.apache.flink.runtime.operators.lifecycle.graph.TestJobBuilders.COMPLEX_GRAPH_BUILDER;
import static org.apache.flink.runtime.operators.lifecycle.graph.TestJobBuilders.SIMPLE_GRAPH_BUILDER;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH;

/**
 * A test suite to check that the operator methods are called according to contract when sources are
 * finishing partially.. The contract was refined in FLIP-147.
 */
@RunWith(Parameterized.class)
public class PartiallyFinishedSourcesITCase extends AbstractTestBase {

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Parameter(0)
    public TestingGraphBuilder graphBuilder;

    @Parameter(1)
    public boolean fullyFinish;

    @Test
    public void test() throws Exception {
        TestJobWithDescription testJob =
                graphBuilder.apply(
                        sharedObjects,
                        cfg -> cfg.setBoolean(ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true),
                        env -> env.setRestartStrategy(fixedDelayRestart(1, 0)));

        TestJobExecutor.submitGraph(testJob.jobGraph)
                .waitForEvent(CheckpointCompletedEvent.class, testJob.eventQueue)
                // finish some sources
                .sendCommand(
                        testJob.commandQueue,
                        FINISH_SOURCES,
                        fullyFinish ? RETAIN_ON_MATCH : REMOVE_ON_MATCH,
                        forOperatorId(testJob.sources.iterator().next()))
                .waitForEvent(CheckpointCompletedEvent.class, testJob.eventQueue)
                // failover
                .sendCommand(testJob.commandQueue, FAIL, REMOVE_ON_MATCH, ALL)
                .waitForEvent(OperatorStartedEvent.class, testJob.eventQueue)
                .waitForAllRunning()
                // finish
                .sendCommand(testJob.commandQueue, FINISH_SOURCES, RETAIN_ON_MATCH, ALL)
                .waitForTermination()
                .execute(miniClusterResource);

        TestJobExecutionValidators.checkOperatorsLifecycle(testJob, true, false);
        TestJobExecutionValidators.checkDataFlow(testJob);
    }

    @Parameterized.Parameters(name = "{0}, fully finish: {1}")
    public static Object[] parameters() {
        return new Object[] {
            new Object[] {SIMPLE_GRAPH_BUILDER, false},
            new Object[] {COMPLEX_GRAPH_BUILDER, false},
            new Object[] {COMPLEX_GRAPH_BUILDER, true},
        };
    }
}
