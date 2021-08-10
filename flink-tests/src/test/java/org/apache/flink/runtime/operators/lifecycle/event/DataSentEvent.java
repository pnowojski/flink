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

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An event of sending some data element downstream usually from {@link
 * org.apache.flink.streaming.api.operators.Input#processElement} method on an operator.
 */
@Internal
public class DataSentEvent extends TestEvent {
    public final long seq;

    public DataSentEvent(String operatorId, int subtaskIndex, long seq) {
        super(operatorId, subtaskIndex);
        this.seq = seq;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataSentEvent)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DataSentEvent that = (DataSentEvent) o;
        return seq == that.seq;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), seq);
    }

    /** LastVertexDataInfo. */
    public static class LastVertexDataInfo implements Serializable {
        public final Map<Integer, Long> bySubtask = new HashMap<>();
    }

    /** LastReceivedVertexDataInfo. */
    public static class LastReceivedVertexDataInfo implements Serializable {
        private final Map<String, LastVertexDataInfo> byUpstreamOperatorID;

        public LastReceivedVertexDataInfo(Map<String, LastVertexDataInfo> byUpstreamOperatorID) {
            this.byUpstreamOperatorID = byUpstreamOperatorID;
        }

        public Map<Integer, Long> forUpstream(String upstreamID) {
            return byUpstreamOperatorID.getOrDefault(upstreamID, new LastVertexDataInfo())
                    .bySubtask;
        }
    }
}
