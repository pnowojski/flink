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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.InputStatus;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Handover wrapper of {@link SourceFunction} into a {@link SourceReaderOperator}.
 *
 * It creates a separate thread for the {@link SourceFunction} and a special handover buffer.
 * Every time SourceFunction collects an element, it is passed to the the handover buffer.
 * This handover buffer is then used by {@link SourceReaderOperator} to emitNext elements.
 */
@Internal
public class SourceFunctionReaderOperator<OUT> extends SourceReaderOperator<OUT> {

	private final StreamSource<OUT, SourceFunction<OUT>> streamSource;

	/**
	 * 	TODO: move to {@link StreamOperatorFactory} and make those fields final.
	 */
	private transient LegacySourceFunctionThread sourceFunctionThread;
	private transient Object checkpointLock;
	private transient Queue<OUT> handoverBuffer;
	private transient HandoverOutput handoverOutput;
	private transient AvailabilityHelper availabilityHelper;
	private transient StreamRecord<OUT> reuse;

	private boolean isFinished;
	@Nullable
	private Throwable asyncException;

	public SourceFunctionReaderOperator(SourceFunction<OUT> sourceFunction) {
		this.streamSource = new StreamSource<>(sourceFunction);
	}

	public SourceFunction<OUT> getSourceFunction() {
		return streamSource.getUserFunction();
	}

	@Override
	public InputStatus emitNext(DataOutput<OUT> output) throws Exception {
		synchronized (checkpointLock) {
			checkErroneous();

			if (!handoverBuffer.isEmpty()) {
				OUT record = handoverBuffer.poll();
				reuse.replace(record);
				output.emitRecord(reuse);
			}

			if (!handoverBuffer.isEmpty()) {
				return InputStatus.MORE_AVAILABLE;
			}
			else if (!isFinished) {
				availabilityHelper.resetUnavailable();
				return InputStatus.NOTHING_AVAILABLE;
			}
			else  {
				return InputStatus.END_OF_INPUT;
			}
		}
	}

	private void checkErroneous() throws FlinkException {
		if (asyncException != null) {
			throw new FlinkException("SourceFunction thread failed.", asyncException);
		}
	}

	@Override
	public void setup(
			StreamTask<?, ?> containingTask,
			StreamConfig config,
			Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);

		handoverBuffer = new ArrayDeque<>();
		handoverOutput = new HandoverOutput();
		availabilityHelper = new AvailabilityHelper();
		reuse = new StreamRecord<OUT>(null);

		streamSource.setup(containingTask, config, output);
		checkpointLock = containingTask.getCheckpointLock();

		sourceFunctionThread = new LegacySourceFunctionThread(containingTask.getStreamStatusMaintainer());
	}

	public void start() {
		sourceFunctionThread.run();
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		checkState(Thread.holdsLock(checkpointLock));
		checkErroneous();
		streamSource.initializeState(context);
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		checkState(Thread.holdsLock(checkpointLock));
		checkErroneous();
		streamSource.snapshotState(context);
	}

	@Override
	public CompletableFuture<?> isAvailable() {
		return availabilityHelper.isAvailable();
	}

	@Override
	public void close() throws Exception {
		sourceFunctionThread.join();
		streamSource.close();
	}

	public void cancel() {
		streamSource.cancel();
	}

	private void legacySourceFunctionThreadCompleted() {
		synchronized (checkpointLock) {
			isFinished = true;
		}
	}

	private void legacySourceFunctionThreadCompleteExceptionally(Throwable t) {
		synchronized (checkpointLock) {
			isFinished = true;
			asyncException = t;
		}
	}

	public void advanceToEndOfEventTime() {
		checkState(Thread.holdsLock(checkpointLock));
		streamSource.advanceToEndOfEventTime();
	}

	public void setTaskDescription(final String taskDescription) {
		sourceFunctionThread.setName("Legacy Source Thread - " + taskDescription);
	}

	public Thread getExecutingThread() {
		return sourceFunctionThread;
	}

	/**
	 * Runnable that executes the the source function in the head operator.
	 */
	private class LegacySourceFunctionThread<OUT> extends Thread {

		private final StreamStatusMaintainer streamStatusMaintainer;

		LegacySourceFunctionThread(StreamStatusMaintainer streamStatusMaintainer) {
			this.streamStatusMaintainer = streamStatusMaintainer;
		}

		@Override
		public void run() {
			try {
				streamSource.run(checkpointLock, streamStatusMaintainer, handoverOutput, null);
				legacySourceFunctionThreadCompleted();
			} catch (Throwable t) {
				legacySourceFunctionThreadCompleteExceptionally(t);
			}
		}

		public void setTaskDescription(final String taskDescription) {
			setName("Legacy Source Thread - " + taskDescription);
		}
	}

	/**
	 * Calls to the methods of this class are protected by {@code checkpointLock} acquired in
	 * {@link org.apache.flink.streaming.api.operators.StreamSourceContexts.WatermarkContext}.
	 */
	private class HandoverOutput implements Output<StreamRecord<OUT>> {
		@Override
		public void emitWatermark(Watermark mark) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void collect(StreamRecord<OUT> record) {
			boolean wasEmpty = handoverBuffer.isEmpty();
			handoverBuffer.add(record.getValue());
			if (wasEmpty) {
				availabilityHelper.getUnavailableToResetAvailable().complete(null);
			}
		}

		@Override
		public void close() {
		}
	}
}
