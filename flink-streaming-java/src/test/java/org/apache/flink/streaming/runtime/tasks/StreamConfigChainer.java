/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Helper class to build StreamConfig for chain of operators.
 */
public class StreamConfigChainer {
	private final StreamConfig headConfig;
	private final Map<Integer, StreamConfig> chainedConfigs = new HashMap<>();

	private StreamConfig tailConfig;
	private TypeSerializer<?> tailOutputSerializer;
	private int chainIndex = 0;

	public StreamConfigChainer(
			OperatorID headOperatorID,
			StreamOperator<?> headOperator,
			StreamConfig headConfig,
			TypeSerializer<?> headOutputSerializer) {
		this.headConfig = checkNotNull(headConfig);
		this.tailConfig = checkNotNull(headConfig);
		this.tailOutputSerializer = checkNotNull(headOutputSerializer);

		head(headOperatorID, headOperator, headOutputSerializer);
	}

	private void head(OperatorID headOperatorID, StreamOperator<?> headOperator, TypeSerializer<?> headOutputSerializer) {
		this.headConfig.setStreamOperator(headOperator);
		this.headConfig.setOperatorID(headOperatorID);
		this.headConfig.setChainStart();
		this.headConfig.setChainIndex(chainIndex);
		this.headConfig.setTypeSerializerOut(headOutputSerializer);
	}

	public <OUT> StreamConfigChainer chain(
			OperatorID operatorID,
			OneInputStreamOperator<?, OUT> operator,
			TypeSerializer<OUT> outputSerializer) {
		chainIndex++;

		tailConfig.setChainedOutputs(Collections.singletonList(
			new StreamEdge(
				new StreamNode(null, tailConfig.getChainIndex(), null, null, null, null, null),
				new StreamNode(null, chainIndex, null, null, null, null, null),
				0,
				Collections.<String>emptyList(),
				null,
				null)));
		tailConfig = new StreamConfig(new Configuration());
		tailConfig.setStreamOperator(checkNotNull(operator));
		tailConfig.setOperatorID(checkNotNull(operatorID));
		tailConfig.setTypeSerializerIn1(tailOutputSerializer);
		tailConfig.setTypeSerializerOut(checkNotNull(outputSerializer));
		tailConfig.setChainIndex(chainIndex);
		tailOutputSerializer = outputSerializer;

		chainedConfigs.put(chainIndex, tailConfig);

		return this;
	}

	public void finish() {

		List<StreamEdge> outEdgesInOrder = new LinkedList<StreamEdge>();
		outEdgesInOrder.add(
			new StreamEdge(
				new StreamNode(null, chainIndex, null, null, null, null, null),
				new StreamNode(null, chainIndex , null, null, null, null, null),
				0,
				Collections.<String>emptyList(),
				new BroadcastPartitioner<Object>(),
				null));

		tailConfig.setBufferTimeout(0);
		tailConfig.setChainEnd();
		tailConfig.setOutputSelectors(Collections.emptyList());
		tailConfig.setNumberOfOutputs(1);
		tailConfig.setOutEdgesInOrder(outEdgesInOrder);
		tailConfig.setNonChainedOutputs(outEdgesInOrder);

		chainedConfigs.put(chainIndex, tailConfig);

		headConfig.setTransitiveChainedTaskConfigs(chainedConfigs);
		headConfig.setOutEdgesInOrder(outEdgesInOrder);
	}
}
