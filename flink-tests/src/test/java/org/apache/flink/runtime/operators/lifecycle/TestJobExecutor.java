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

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommand;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandQueue;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandQueue.TestCommandRetention;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandQueue.TestCommandTarget;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue.TestEventHandler.TestEventNextAction.CONTINUE;
import static org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue.TestEventHandler.TestEventNextAction.STOP;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;

class TestJobExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TestJobExecutor.class);

    private final List<ThrowingConsumer<TestJobExecutionContext, Exception>> steps;

    private TestJobExecutor(List<ThrowingConsumer<TestJobExecutionContext, Exception>> steps) {
        this.steps = steps;
    }

    public void execute(MiniClusterWithClientResource miniClusterResource) throws Exception {
        TestJobExecutionContext context = new TestJobExecutionContext(miniClusterResource);
        for (ThrowingConsumer<TestJobExecutionContext, Exception> step : steps) {
            step.accept(context);
        }
    }

    public static TestJobExecutor submitGraph(JobGraph jobGraph) {
        List<ThrowingConsumer<TestJobExecutionContext, Exception>> steps = new ArrayList<>();
        steps.add(
                ctx -> {
                    LOG.debug("submitGraph: {}", jobGraph);
                    MiniClusterWithClientResource miniClusterResource = ctx.miniClusterResource;
                    ctx.job = miniClusterResource.getClusterClient().submitJob(jobGraph).get();
                    waitForAllTaskRunning(miniClusterResource.getMiniCluster(), ctx.job);
                });
        return new TestJobExecutor(steps);
    }

    public TestJobExecutor waitForAllRunning() {
        List<ThrowingConsumer<TestJobExecutionContext, Exception>> steps =
                new ArrayList<>(this.steps);
        steps.add(
                ctx -> {
                    LOG.debug("waitForAllRunning in {}", ctx.job);
                    waitForAllTaskRunning(ctx.miniClusterResource.getMiniCluster(), ctx.job);
                });
        return new TestJobExecutor(steps);
    }

    public TestJobExecutor waitForEvent(
            Class<? extends TestEvent> eventClass, TestEventQueue eventQueue) {
        List<ThrowingConsumer<TestJobExecutionContext, Exception>> steps =
                new ArrayList<>(this.steps);
        steps.add(
                ctx -> {
                    LOG.debug("waitForEvent: {}", eventClass.getSimpleName());
                    eventQueue.withHandler(
                            e -> eventClass.isAssignableFrom(e.getClass()) ? STOP : CONTINUE);
                });
        return new TestJobExecutor(steps);
    }

    public TestJobExecutor stopWithSavepoint(TemporaryFolder folder, boolean withDrain) {
        List<ThrowingConsumer<TestJobExecutionContext, Exception>> steps =
                new ArrayList<>(this.steps);
        steps.add(
                ctx -> {
                    LOG.debug("stopWithSavepoint: {} (withDrain: {})", folder, withDrain);
                    ClusterClient<?> client = ctx.miniClusterResource.getClusterClient();
                    client.stopWithSavepoint(ctx.job, withDrain, folder.newFolder().toString())
                            .get();
                });
        return new TestJobExecutor(steps);
    }

    public TestJobExecutor sendCommand(
            TestCommandQueue commandQueue,
            TestCommand testCommand,
            TestCommandRetention retention,
            TestCommandTarget target) {
        steps.add(
                ctx -> {
                    LOG.debug("sendCommand: {}", testCommand);
                    commandQueue.add(testCommand, target, retention);
                });
        return this;
    }

    public TestJobExecutor waitForTermination() {
        steps.add(
                ctx -> {
                    LOG.debug("waitForTermination");
                    while (!ctx.miniClusterResource
                            .getClusterClient()
                            .getJobStatus(ctx.job)
                            .get()
                            .isGloballyTerminalState()) {
                        Thread.sleep(100);
                    }
                });
        return this;
    }

    private static class TestJobExecutionContext {
        private final MiniClusterWithClientResource miniClusterResource;
        @Nullable JobID job;

        private TestJobExecutionContext(MiniClusterWithClientResource miniClusterResource) {
            this.miniClusterResource = miniClusterResource;
        }
    }
}
