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

import org.apache.flink.runtime.operators.lifecycle.event.CheckpointCompletedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointStartedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.DataSentEvent;
import org.apache.flink.runtime.operators.lifecycle.event.InputEndedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorFinishedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;
import org.apache.flink.runtime.operators.lifecycle.event.WatermarkReceivedEvent;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;

import java.util.HashMap;
import java.util.Map;

class OneInputTestStreamOperator extends AbstractStreamOperator<TestEvent>
        implements OneInputStreamOperator<TestEvent, TestEvent>,
                BoundedOneInput,
                ProcessingTimeCallback {
    private final String operatorID;
    private long lastDataSent;
    private final Map<String, DataSentEvent.LastVertexDataInfo> lastDataReceived = new HashMap<>();
    private boolean timerRegistered;
    private final TestEventQueue eventQueue;

    OneInputTestStreamOperator(String operatorID, TestEventQueue eventQueue) {
        this.operatorID = operatorID;
        this.eventQueue = eventQueue;
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        eventQueue.add(
                new CheckpointStartedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        context.getCheckpointId()));
        super.snapshotState(context);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        eventQueue.add(
                new CheckpointCompletedEvent(
                        operatorID, getRuntimeContext().getIndexOfThisSubtask(), checkpointId));
        super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void finish() throws Exception {
        eventQueue.add(
                new OperatorFinishedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        lastDataSent,
                        new DataSentEvent.LastReceivedVertexDataInfo(lastDataReceived)));
        super.finish();
    }

    @Override
    public void processElement(StreamRecord<TestEvent> element) throws Exception {
        TestEvent e = element.getValue();
        if (e instanceof DataSentEvent) {
            lastDataReceived
                    .computeIfAbsent(e.operatorId, ign -> new DataSentEvent.LastVertexDataInfo())
                    .bySubtask
                    .put(e.subtaskIndex, ((DataSentEvent) e).seq);
            output.collect(
                    new StreamRecord<>(
                            new DataSentEvent(
                                    operatorID,
                                    getRuntimeContext().getIndexOfThisSubtask(),
                                    ++lastDataSent)));
        } else {
            output.collect(new StreamRecord<>(e));
        }
        if (!timerRegistered) {
            registerTimer();
            timerRegistered = true;
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        eventQueue.add(
                new WatermarkReceivedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        mark.getTimestamp(),
                        1));
        super.processWatermark(mark);
    }

    @Override
    public void endInput() throws Exception {
        eventQueue.add(
                new InputEndedEvent(operatorID, getRuntimeContext().getIndexOfThisSubtask(), 1));
    }

    @Override
    public void onProcessingTime(long timestamp) {
        registerTimer();
    }

    private void registerTimer() {
        getProcessingTimeService()
                .registerTimer(getProcessingTimeService().getCurrentProcessingTime() + 1, this);
    }
}
