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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.TestInputChannel;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT;
import static org.apache.flink.runtime.io.network.api.serialization.EventSerializer.toBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

/**
 * {@link AlternatingCheckpointBarrierHandler} test.
 */
public class AlternatingCheckpointBarrierHandlerTest {

	@Test
	public void testCheckpointHandling() throws Exception {
		testBarrierHandling(CHECKPOINT);
	}

	@Test
	public void testSavepointHandling() throws Exception {
		testBarrierHandling(SAVEPOINT);
	}

	@Test
	public void testAlternation() throws Exception {
		int numBarriers = 123;
		int numChannels = 123;
		TestInvokable target = new TestInvokable();
		CheckpointedInputGate gate = buildGate(target, numChannels);
		List<Long> barriers = new ArrayList<>();
		for (long barrier = 0; barrier < numBarriers; barrier++) {
			barriers.add(barrier);
			CheckpointType type = barrier % 2 == 0 ? CHECKPOINT : SAVEPOINT;
			for (int channel = 0; channel < numChannels; channel++) {
				sendBarrier(barrier, type, (TestInputChannel) gate.getChannel(channel), gate);
			}
		}
		assertEquals(barriers, target.triggeredCheckpoints);
	}

	@Test
	public void testPreviousHandlerReset() throws Exception {
		SingleInputGate inputGate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
		inputGate.setInputChannels(new TestInputChannel(inputGate, 0), new TestInputChannel(inputGate, 1));
		TestInvokable target = new TestInvokable();
		CheckpointBarrierAligner alignedHandler = new CheckpointBarrierAligner("test", new InputGate[]{inputGate, inputGate}, singletonMap(inputGate, 0), target);
		CheckpointBarrierUnaligner unalignedHandler = new CheckpointBarrierUnaligner(new int[]{inputGate.getNumberOfInputChannels()}, ChannelStateWriter.NO_OP, "test", target);
		AlternatingCheckpointBarrierHandler barrierHandler = new AlternatingCheckpointBarrierHandler(alignedHandler, unalignedHandler, target);

		for (int i = 0; i < 4; i++) {
			int channel = i % 2;
			CheckpointType type = channel == 0 ? CHECKPOINT : SAVEPOINT;
			barrierHandler.processBarrier(new CheckpointBarrier(i, 0, new CheckpointOptions(type, CheckpointStorageLocationReference.getDefault())), channel);
			assertEquals(type.isSavepoint(), alignedHandler.isCheckpointPending());
			assertNotEquals(alignedHandler.isCheckpointPending(), unalignedHandler.isCheckpointPending());

			if (!type.isSavepoint()) {
				assertFalse(barrierHandler.getAllBarriersReceivedFuture(i).isDone());
				assertInflightDataEquals(unalignedHandler, barrierHandler, i, inputGate.getNumberOfInputChannels());
			}
		}
	}

	private static void assertInflightDataEquals(CheckpointBarrierHandler expected, CheckpointBarrierHandler actual, long barrierId, int numChannels) {
		for (int channelId = 0; channelId < numChannels; channelId++) {
			assertEquals(expected.hasInflightData(barrierId, channelId), actual.hasInflightData(barrierId, channelId));
		}
	}

	@Test
	public void testHasInflightDataBeforeProcessBarrier() throws Exception {
		SingleInputGate inputGate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
		inputGate.setInputChannels(new TestInputChannel(inputGate, 0), new TestInputChannel(inputGate, 1));
		TestInvokable target = new TestInvokable();
		CheckpointBarrierAligner alignedHandler = new CheckpointBarrierAligner("test", new InputGate[]{inputGate, inputGate}, singletonMap(inputGate, 0), target);
		CheckpointBarrierUnaligner unalignedHandler = new CheckpointBarrierUnaligner(new int[]{inputGate.getNumberOfInputChannels()}, ChannelStateWriter.NO_OP, "test", target);
		AlternatingCheckpointBarrierHandler barrierHandler = new AlternatingCheckpointBarrierHandler(alignedHandler, unalignedHandler, target);

		final long id = 1;
		unalignedHandler.processBarrier(new CheckpointBarrier(id, 0, new CheckpointOptions(CHECKPOINT, CheckpointStorageLocationReference.getDefault())), 0);

		assertInflightDataEquals(unalignedHandler, barrierHandler, id, inputGate.getNumberOfInputChannels());
		assertFalse(barrierHandler.getAllBarriersReceivedFuture(id).isDone());
	}

	@Test
	public void testEndOfPartition() throws Exception {
		int totalChannels = 5;
		int closedChannels = 2;
		SingleInputGate inputGate = new SingleInputGateBuilder().setNumberOfChannels(totalChannels).build();
		TestInvokable target = new TestInvokable();
		CheckpointBarrierAligner alignedHandler = new CheckpointBarrierAligner("test", new InputGate[]{inputGate}, singletonMap(inputGate, 0), target);
		CheckpointBarrierUnaligner unalignedHandler = new CheckpointBarrierUnaligner(new int[]{inputGate.getNumberOfInputChannels()}, ChannelStateWriter.NO_OP, "test", target);
		AlternatingCheckpointBarrierHandler barrierHandler = new AlternatingCheckpointBarrierHandler(alignedHandler, unalignedHandler, target);
		for (int i = 0; i < closedChannels; i++) {
			barrierHandler.processEndOfPartition();
		}
		assertEquals(closedChannels, alignedHandler.getNumClosedChannels());
		assertEquals(totalChannels - closedChannels, unalignedHandler.getNumOpenChannels());
	}

	private void testBarrierHandling(CheckpointType checkpointType) throws Exception {
		final long barrierId = 123L;
		TestInvokable target = new TestInvokable();
		SingleInputGate gate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
		TestInputChannel fast = new TestInputChannel(gate, 0, false, true);
		TestInputChannel slow = new TestInputChannel(gate, 1, false, true);
		gate.setInputChannels(fast, slow);
		AlternatingCheckpointBarrierHandler barrierHandler = barrierHandler(gate, target);
		CheckpointedInputGate checkpointedGate = new CheckpointedInputGate(gate, barrierHandler, 0 /* offset */);

		sendBarrier(barrierId, checkpointType, fast, checkpointedGate);

		assertEquals(checkpointType.isSavepoint(), target.triggeredCheckpoints.isEmpty());
		assertEquals(checkpointType.isSavepoint(), barrierHandler.isBlocked(fast.getChannelIndex()));
		assertFalse(barrierHandler.isBlocked(slow.getChannelIndex()));

		sendBarrier(barrierId, checkpointType, slow, checkpointedGate);

		assertEquals(singletonList(barrierId), target.triggeredCheckpoints);
		for (InputChannel channel : gate.getInputChannels().values()) {
			assertFalse(barrierHandler.isBlocked(channel.getChannelIndex()));
			assertEquals(
				String.format("channel %d should be resumed", channel.getChannelIndex()),
				checkpointType.isSavepoint(),
				((TestInputChannel) channel).isResumed());
		}
	}

	private void sendBarrier(long id, CheckpointType type, TestInputChannel channel, CheckpointedInputGate gate) throws Exception {
		channel.read(barrier(id, type).retainBuffer());
		while (gate.pollNext().isPresent()) {
		}
	}

	private static AlternatingCheckpointBarrierHandler barrierHandler(SingleInputGate inputGate, AbstractInvokable target) {
		String taskName = "test";
		InputGate[] channelIndexToInputGate = new InputGate[inputGate.getNumberOfInputChannels()];
		Arrays.fill(channelIndexToInputGate, inputGate);
		return new AlternatingCheckpointBarrierHandler(
			new CheckpointBarrierAligner(taskName, channelIndexToInputGate, singletonMap(inputGate, 0), target),
			new CheckpointBarrierUnaligner(new int[]{inputGate.getNumberOfInputChannels()}, ChannelStateWriter.NO_OP, taskName, target),
			target);
	}

	private Buffer barrier(long id, CheckpointType checkpointType) throws IOException {
		return toBuffer(new CheckpointBarrier(
			id,
			System.currentTimeMillis(),
			new CheckpointOptions(checkpointType, CheckpointStorageLocationReference.getDefault(), true, true)));
	}

	private static class TestInvokable extends AbstractInvokable {
		private List<Long> triggeredCheckpoints = new ArrayList<>();

		TestInvokable() {
			super(new DummyEnvironment());
		}

		@Override
		public void invoke() {
		}

		@Override
		public <E extends Exception> void executeInTaskThread(ThrowingRunnable<E> runnable, String descriptionFormat, Object... descriptionArgs) throws E {
			runnable.run();
		}

		@Override
		public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) {
			triggeredCheckpoints.add(checkpointMetaData.getCheckpointId());
		}

		@Override
		public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) {
		}
	}

	private static CheckpointedInputGate buildGate(TestInvokable target, int numChannels) {
		SingleInputGate gate = new SingleInputGateBuilder().setNumberOfChannels(numChannels).build();
		TestInputChannel[] channels = new TestInputChannel[numChannels];
		for (int i = 0; i < numChannels; i++) {
			channels[i] = new TestInputChannel(gate, i, false, true);
		}
		gate.setInputChannels(channels);
		return new CheckpointedInputGate(gate, barrierHandler(gate, target), 0);
	}

}
