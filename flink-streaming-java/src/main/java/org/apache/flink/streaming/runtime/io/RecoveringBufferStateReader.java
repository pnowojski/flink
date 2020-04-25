/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@link BufferStateReader} that is recovering data from {@link ChannelStateReader}.
 */
@Internal
public class RecoveringBufferStateReader extends BufferStateReader {
	private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();
	private final ExecutorService unspillingExecutor = Executors.newSingleThreadExecutor(new ExecutorThreadFactory("channel-state-unspilling"));
	private final ChannelStateReader channelStateReader;
	private final ArrayDeque<BufferOrEvent> recoveredBuffers = new ArrayDeque<>();
	private final int recoveredBuffersLimit = 10;

	private @Nullable Exception asyncException = null;
	private boolean unspillingFinished = false;
	private boolean running = true;

	public RecoveringBufferStateReader(
			Map<InputGate, Integer> inputGateToChannelIndexOffset,
			ChannelStateReader channelStateReader) {
		super(false);
		this.channelStateReader = channelStateReader;

		startUnspilling(inputGateToChannelIndexOffset);
	}

	private void startUnspilling(Map<InputGate, Integer> inputGateToChannelIndexOffset) {
		unspillingExecutor.submit(() -> readInputData(inputGateToChannelIndexOffset));
	}

	private void readInputData(Map<InputGate, Integer> channelIndexToInputGate) {
		try {
			for (Map.Entry<InputGate, Integer> entry : channelIndexToInputGate.entrySet()) {
				InputGate inputGate = entry.getKey();
				Integer channelIndexOffset = entry.getValue();
				for (int channelIndex = 0; channelIndex < inputGate.getNumberOfInputChannels(); channelIndex++) {
					InputChannelInfo info = inputGate.getChannel(channelIndex).getChannelInfo();
					ChannelStateReader.ReadResult result;
					do {
						// TODO: what should be the buffer size?
						// TODO: pool memory segments from NetworkBufferPool?
						Buffer buffer = new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(32 * 1024), FreeingBufferRecycler.INSTANCE);
						result = channelStateReader.readInputData(info, buffer);
						enqueueBuffer(buffer, channelIndexOffset + channelIndex);
						if (!running) {
							return;
						}
					} while (result == ChannelStateReader.ReadResult.HAS_MORE_DATA);
				}
			}
		}
		catch (Exception ex) {
			synchronized (recoveredBuffers) {
				asyncException = ex;
			}
		}
		finally {
			synchronized (recoveredBuffers) {
				unspillingFinished = true;
			}
		}
	}

	private void enqueueBuffer(Buffer buffer, int channelIndex) throws InterruptedException {
		CompletableFuture<?> toNotify = null;

		synchronized (recoveredBuffers) {
			if (!running) {
				buffer.recycleBuffer();
				return;
			}
			if (recoveredBuffers.isEmpty()) {
				toNotify = availabilityHelper.getUnavailableToResetAvailable();
			}
			recoveredBuffers.add(new BufferOrEvent(buffer, channelIndex));
			if (isOverTheLimit()) {
				recoveredBuffers.wait();
			}
		}

		if (toNotify != null) {
			toNotify.complete(null);
		}
	}

	private boolean isOverTheLimit() {
		return recoveredBuffers.size() > recoveredBuffersLimit;
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws Exception {
		if (finished) {
			return Optional.empty();
		}
		synchronized (recoveredBuffers) {
			if (asyncException != null) {
				throw asyncException;
			}
			if (recoveredBuffers.isEmpty()) {
				availabilityHelper.resetUnavailable();
				finished = unspillingFinished;
				return Optional.empty();
			}
			boolean wasOverTheLimit = isOverTheLimit();
			BufferOrEvent bufferOrEvent = recoveredBuffers.poll();
			if (wasOverTheLimit && !isOverTheLimit()) {
				recoveredBuffers.notifyAll();
			}
			bufferOrEvent.setMoreAvailable(recoveredBuffers.isEmpty());
			return Optional.of(bufferOrEvent);
		}
	}

	@Override
	public boolean isFinished() {
		return finished;
	}

	@Override
	public void close() throws Exception {
		synchronized (recoveredBuffers) {
			for (BufferOrEvent buffer : recoveredBuffers) {
				buffer.getBuffer().recycleBuffer();
			}
			running = false;
			unspillingExecutor.shutdown();
		}
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (isFinished()) {
			return AVAILABLE;
		}
		return availabilityHelper.getAvailableFuture();
	}
}
