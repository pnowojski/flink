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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;

import java.util.Map;
import java.util.Set;

/** Test {@link JobGraph} with description to allow its execution validation. */
public class TestJobWithDescription {
    final JobGraph jobGraph;
    final Set<String> operatorsWithLifecycleTracking;
    final Set<String> operatorsWithDataFlowTracking;
    final Map<String, Integer> operatorsNumberOfInputs;
    final TestEventQueue eventQueue;

    public TestJobWithDescription(
            JobGraph jobGraph,
            Set<String> operatorsWithLifecycleTracking,
            Set<String> operatorsWithDataFlowTracking,
            Map<String, Integer> operatorsNumberOfInputs,
            TestEventQueue eventQueue) {
        this.jobGraph = jobGraph;
        this.operatorsWithLifecycleTracking = operatorsWithLifecycleTracking;
        this.operatorsWithDataFlowTracking = operatorsWithDataFlowTracking;
        this.operatorsNumberOfInputs = operatorsNumberOfInputs;
        this.eventQueue = eventQueue;
    }
}
