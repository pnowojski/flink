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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava18.com.google.common.collect.LinkedListMultimap;
import org.apache.flink.shaded.guava18.com.google.common.collect.ListMultimap;

import java.util.Arrays;

/**
 * A simple {@link ChannelStateWriter} used to write unit tests.
 */
public class RecordingChannelStateWriter extends MockChannelStateWriter {
	private long lastStartedCheckpointId = -1;
	private long lastFinishedCheckpointId = -1;
	private ListMultimap<InputChannelInfo, Buffer> addedInput = LinkedListMultimap.create();
	private ListMultimap<ResultSubpartitionInfo, Buffer> adedOutput = LinkedListMultimap.create();

	public RecordingChannelStateWriter() {
		super(false);
	}

	public void reset() {
		lastStartedCheckpointId = -1;
		lastFinishedCheckpointId = -1;
		addedInput.values().forEach(Buffer::recycleBuffer);
		addedInput.clear();
		adedOutput.values().forEach(Buffer::recycleBuffer);
		adedOutput.clear();
	}

	@Override
	public void start(long checkpointId, CheckpointOptions checkpointOptions) {
		super.start(checkpointId, checkpointOptions);
		lastStartedCheckpointId = checkpointId;
	}

	@Override
	public void addInputData(long checkpointId, InputChannelInfo info, int startSeqNum, CloseableIterator<Buffer> data) {
		checkCheckpointId(checkpointId);
		data.forEachRemaining(b -> addedInput.put(info, b));
	}

	@Override
	public void addOutputData(long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data) {
		checkCheckpointId(checkpointId);
		adedOutput.putAll(info, Arrays.asList(data));
	}

	public long getLastStartedCheckpointId() {
		return lastStartedCheckpointId;
	}

	public long getLastFinishedCheckpointId() {
		return lastFinishedCheckpointId;
	}

	@Override
	public void stop(long checkpointId) {
		lastFinishedCheckpointId = checkpointId;
	}

	public ListMultimap<InputChannelInfo, Buffer> getAddedInput() {
		return addedInput;
	}

	public ListMultimap<ResultSubpartitionInfo, Buffer> getAddedOutput() {
		return adedOutput;
	}
}
