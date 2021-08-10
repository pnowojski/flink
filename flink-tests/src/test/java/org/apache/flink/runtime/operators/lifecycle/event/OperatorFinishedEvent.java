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

package org.apache.flink.runtime.operators.lifecycle.event;

import org.apache.flink.streaming.api.operators.StreamOperator;

/**
 * An event of calling {@link StreamOperator#finish() finish}. Serves two purposes:
 *
 * <ol>
 *   <li>Verify that the call actually happened
 *   <li>Collect sent/receive info to verify that there was no data loss
 * </ol>
 */
public class OperatorFinishedEvent extends TestEvent {
    final DataSentEvent.LastReceivedVertexDataInfo receiveInfo;
    public final long lastSent;

    public OperatorFinishedEvent(
            String operatorId,
            int subtaskIndex,
            long lastSent,
            DataSentEvent.LastReceivedVertexDataInfo receiveInfo) {
        super(operatorId, subtaskIndex);
        this.receiveInfo = receiveInfo;
        this.lastSent = lastSent;
    }

    public long getLastReceived(String upstreamID, int upstreamIndex) {
        return receiveInfo.forUpstream(upstreamID).getOrDefault(upstreamIndex, -1L);
    }
}
