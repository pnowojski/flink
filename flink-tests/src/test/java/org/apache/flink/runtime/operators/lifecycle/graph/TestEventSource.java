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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.lifecycle.event.DataSentEvent;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorFinishedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import static java.util.Collections.emptyMap;

class TestEventSource extends RichSourceFunction<TestEvent>
        implements ParallelSourceFunction<TestEvent> {
    private final String operatorID;
    private transient volatile boolean isRunning = true;
    private final TestEventQueue eventQueue;

    TestEventSource(String operatorID, TestEventQueue eventQueue) {
        this.operatorID = operatorID;
        this.eventQueue = eventQueue;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.isRunning = true;
    }

    @Override
    public void run(SourceContext<TestEvent> ctx) {
        long lastSent = 0;
        while (isRunning) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(
                        new DataSentEvent(
                                operatorID,
                                getRuntimeContext().getIndexOfThisSubtask(),
                                ++lastSent));
            }
        }
        // note: this only gets collected with FLIP-147 changes
        synchronized (ctx.getCheckpointLock()) {
            eventQueue.add(
                    new OperatorFinishedEvent(
                            operatorID,
                            getRuntimeContext().getIndexOfThisSubtask(),
                            lastSent,
                            new DataSentEvent.LastReceivedVertexDataInfo(emptyMap())));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
