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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointCompletedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointStartedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.InputEndedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorFinishedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;
import org.apache.flink.runtime.operators.lifecycle.event.WatermarkReceivedEvent;
import org.apache.flink.streaming.api.watermark.Watermark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

class TestJobExecutionValidators {
    private static final Logger LOG = LoggerFactory.getLogger(TestJobExecutionValidators.class);

    private TestJobExecutionValidators() {}

    public interface TestJobExecutionValidator extends Consumer<TestJobWithDescription> {}

    static void checkOperatorsLifecycle(
            TestJobWithDescription testJob, boolean withDrain, boolean finishOnSameCheckpointID) {
        List<TestEvent> events = testJob.eventQueue.getAll();
        OptionalLong lastCheckpointID =
                finishOnSameCheckpointID
                        ? events.stream()
                                .filter(e -> e instanceof CheckpointCompletedEvent)
                                .mapToLong(e -> ((CheckpointCompletedEvent) e).checkpointID)
                                .max()
                        : OptionalLong.empty();
        long lastWatermarkTs = withDrain ? Watermark.MAX_WATERMARK.getTimestamp() : -1;
        // not checking if the watermark was the same if !withDrain
        //                        : events.stream()
        //                                .filter(e -> e instanceof WatermarkReceivedEvent)
        //                                .mapToLong(e -> ((WatermarkReceivedEvent) e).ts)
        //                                .max()
        //                                .getAsLong();
        Map<Tuple2<String, Integer>, List<TestEvent>> eventsByOperator = new HashMap<>();
        for (TestEvent ev : events) {
            eventsByOperator
                    .computeIfAbsent(
                            Tuple2.of(ev.operatorId, ev.subtaskIndex), ign -> new ArrayList<>())
                    .add(ev);
        }
        eventsByOperator.forEach(
                (operatorIdAndIndex, operatorEvents) -> {
                    String id = operatorIdAndIndex.f0;
                    if (testJob.operatorsWithLifecycleTracking.contains(id)) {
                        checkOperatorsLifecycle(
                                testJob,
                                withDrain,
                                lastCheckpointID,
                                lastWatermarkTs,
                                operatorEvents,
                                id,
                                operatorIdAndIndex.f1);
                    }
                });
    }

    private static void checkOperatorsLifecycle(
            TestJobWithDescription testJob,
            boolean withDrain,
            OptionalLong lastCheckpointID,
            long lastWatermarkTs,
            List<TestEvent> operatorEvents,
            String id,
            int index) {
        if (withDrain) {
            validateDraining(testJob, lastWatermarkTs, operatorEvents, id, index);
        }
        if (withDrain) {
            // currently (1.14), finish is only called withDrain
            // todo: enable after updating production code
            validateFinishing(operatorEvents, id, index);
        }
        if (withDrain) {
            validateFinalCheckpoint(operatorEvents, id, index);
        }
        lastCheckpointID.ifPresent(
                cpId -> validateFinalCheckpoint(cpId, operatorEvents, id, index));
    }

    private static void validateFinalCheckpoint(
            List<TestEvent> operatorEvents, String id, int index) {
        assertTrue(operatorEvents.size() >= 2);
        TestEvent last = operatorEvents.get(operatorEvents.size() - 1);
        assertEquals(
                format(
                        "Operator %s[%d] has wrong last event (all events: %s)",
                        id, index, operatorEvents),
                CheckpointCompletedEvent.class,
                last.getClass());
    }

    private static void validateFinalCheckpoint(
            long lastCheckpointID, List<TestEvent> operatorEvents, String id, int index) {
        boolean started = false;
        boolean finished = false;
        for (TestEvent ev : operatorEvents) {
            if (ev instanceof CheckpointStartedEvent) {
                if (lastCheckpointID == ((CheckpointStartedEvent) ev).checkpointID) {
                    assertFalse(
                            format(
                                    "Operator %s[%d] started checkpoint %d twice",
                                    id, index, lastCheckpointID),
                            started);
                    started = true;
                }
            } else if (ev instanceof CheckpointCompletedEvent) {
                if (lastCheckpointID == ((CheckpointCompletedEvent) ev).checkpointID) {
                    assertTrue(
                            format(
                                    "Operator %s[%d] finished checkpoint %d before starting",
                                    id, index, lastCheckpointID),
                            started);
                    assertFalse(
                            format(
                                    "Operator %s[%d] finished checkpoint %d twice",
                                    id, index, lastCheckpointID),
                            finished);
                    finished = true;
                }
            }
        }
        assertTrue(
                format(
                        "Operator %s[%d] didn't finish checkpoint %d (events: %s)",
                        id, index, lastCheckpointID, operatorEvents),
                finished);
    }

    private static void validateFinishing(List<TestEvent> operatorEvents, String id, int index) {
        boolean finished = false;
        for (TestEvent ev : operatorEvents) {
            if (ev instanceof OperatorFinishedEvent) {
                finished = true;
            } else if (finished) {
                assertTrue(
                        format(
                                "Unexpected event after operator %s[%d] finished: %s",
                                id, index, ev),
                        ev instanceof CheckpointStartedEvent
                                || ev instanceof CheckpointCompletedEvent);
            }
        }
        assertTrue(format("Operator %s[%d] wasn't finished", id, index), finished);
    }

    private static void validateDraining(
            TestJobWithDescription testJob,
            long lastWatermarkTs,
            List<TestEvent> operatorEvents,
            String id,
            int index) {
        BitSet endedInputs = new BitSet();
        BitSet inputsWithMaxWatermark = new BitSet();
        for (TestEvent ev : operatorEvents) {
            if (ev instanceof WatermarkReceivedEvent) {
                WatermarkReceivedEvent w = (WatermarkReceivedEvent) ev;
                if (w.ts == lastWatermarkTs) {
                    assertFalse(inputsWithMaxWatermark.get(w.inputId));
                    inputsWithMaxWatermark.set(w.inputId);
                }
            } else if (ev instanceof InputEndedEvent) {
                InputEndedEvent w = (InputEndedEvent) ev;
                assertTrue(
                        format(
                                "Input %d ended before receiving %d watermark by %s[%d]",
                                w.inputId, lastWatermarkTs, id, index),
                        inputsWithMaxWatermark.get(w.inputId));
                assertFalse(endedInputs.get(w.inputId));
                endedInputs.set(w.inputId);
            }
        }
        assertEquals(
                format("Incorrect number of ended inputs for %s[%d]", id, index),
                getNumInputs(testJob, id),
                endedInputs.cardinality());
    }

    private static int getNumInputs(TestJobWithDescription testJob, String operator) {
        Integer explicitNumInputs = testJob.operatorsNumberOfInputs.get(operator);
        if (explicitNumInputs != null) {
            return explicitNumInputs;
        }
        Iterable<JobVertex> vertices = testJob.jobGraph.getVertices();
        for (JobVertex vertex : vertices) {
            for (OperatorIDPair p : vertex.getOperatorIDs()) {
                OperatorID operatorID =
                        p.getUserDefinedOperatorID().orElse(p.getGeneratedOperatorID());
                if (operatorID.toString().equals(operator)) {
                    // warn: this returns the number of network inputs
                    // which may not coincide with logical
                    // e.g. single-input operator after two sources united
                    // will have two network inputs
                    return vertex.getNumberOfInputs();
                }
            }
        }
        throw new NoSuchElementException(operator);
    }

    /** Check that all data from the upstream reached the respective downstreams. */
    static void checkDataFlow(TestJobWithDescription testJob) {
        Map<String, Map<Integer, OperatorFinishedEvent>> finishEvents = new HashMap<>();
        for (TestEvent ev : testJob.eventQueue.getAll()) {
            if (ev instanceof OperatorFinishedEvent) {
                finishEvents
                        .computeIfAbsent(ev.operatorId, ign -> new HashMap<>())
                        .put(ev.subtaskIndex, ((OperatorFinishedEvent) ev));
            }
        }

        for (JobVertex upstream : testJob.jobGraph.getVertices()) {
            for (IntermediateDataSet produced : upstream.getProducedDataSets()) {
                for (JobEdge edge : produced.getConsumers()) {
                    Optional<String> upstreamID = getTrackedOperatorID(upstream, true, testJob);
                    Optional<String> downstreamID =
                            getTrackedOperatorID(edge.getTarget(), false, testJob);
                    if (upstreamID.isPresent() && downstreamID.isPresent()) {
                        checkDataFlow(upstreamID.get(), downstreamID.get(), edge, finishEvents);
                    } else {
                        LOG.debug("Ignoring edge (untracked operator): {}", edge);
                    }
                }
            }
        }
    }

    /**
     * Check that for each upstream subtask there exists a downstream subtask that received it's
     * latest emitted element.
     */
    private static void checkDataFlow(
            String upstreamID,
            String downstreamID,
            JobEdge edge,
            Map<String, Map<Integer, OperatorFinishedEvent>> finishEvents) {
        LOG.debug(
                "Checking {} edge\n  from {} ({})\n  to {} ({})",
                edge.getDistributionPattern(),
                edge.getSource().getProducer().getName(),
                upstreamID,
                edge.getTarget().getName(),
                downstreamID);

        Map<Integer, OperatorFinishedEvent> downstreamFinishInfo =
                getForOperator(downstreamID, finishEvents);

        Map<Integer, OperatorFinishedEvent> upstreamFinishInfo =
                getForOperator(upstreamID, finishEvents);

        upstreamFinishInfo.forEach(
                (upstreamIndex, upstreamInfo) ->
                        assertTrue(
                                anySubtaskReceived(
                                        upstreamID,
                                        upstreamIndex,
                                        upstreamInfo.lastSent,
                                        downstreamFinishInfo.values())));
    }

    private static boolean anySubtaskReceived(
            String upstreamID,
            int upstreamIndex,
            long upstreamValue,
            Collection<OperatorFinishedEvent> downstreamFinishInfo) {
        return downstreamFinishInfo.stream()
                .anyMatch(
                        event -> event.getLastReceived(upstreamID, upstreamIndex) == upstreamValue);
    }

    private static Map<Integer, OperatorFinishedEvent> getForOperator(
            String operatorId, Map<String, Map<Integer, OperatorFinishedEvent>> finishEvents) {
        Map<Integer, OperatorFinishedEvent> events = finishEvents.get(operatorId);
        assertNotNull(
                format(
                        "Operator finish info wasn't collected: %s (collected: %s)",
                        operatorId, finishEvents),
                events);
        return events;
    }

    /**
     * Traverse operators in the chain in the vertex and return the first tracked operator ID. For
     * upstream, start with head, for downstream - with tail (see {@link
     * JobVertex#getOperatorIDs()}). If a chain doesn't contain any tracked operators return
     * nothing.
     */
    private static Optional<String> getTrackedOperatorID(
            JobVertex vertex, boolean upstream, TestJobWithDescription testJob) {
        ListIterator<OperatorIDPair> iterator =
                vertex.getOperatorIDs().listIterator(upstream ? 0 : vertex.getOperatorIDs().size());
        while (upstream ? iterator.hasNext() : iterator.hasPrevious()) {
            OperatorIDPair idPair = upstream ? iterator.next() : iterator.previous();
            String id =
                    idPair.getUserDefinedOperatorID()
                            .orElse(idPair.getGeneratedOperatorID())
                            .toString();
            if (testJob.operatorsWithDataFlowTracking.contains(id)) {
                return Optional.of(id);
            }
        }
        return Optional.empty();
    }
}
