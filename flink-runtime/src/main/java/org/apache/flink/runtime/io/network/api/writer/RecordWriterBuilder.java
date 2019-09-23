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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Utility class to encapsulate the logic of building a {@link RecordWriter} instance.
 */
public class RecordWriterBuilder<T extends IOReadableWritable> {

	private ChannelSelector<T> selector = new RoundRobinChannelSelector<>();

	private Function<Runnable, Future<?>> flushSubmit = this::runLocally;

	private long timeout = -1;

	private String taskName = "test";

	public RecordWriterBuilder<T> setChannelSelector(ChannelSelector<T> selector) {
		this.selector = selector;
		return this;
	}

	public RecordWriterBuilder<T> setTimeout(long timeout) {
		return setupOutpuFlusher(this::runLocally, timeout);
	}

	public RecordWriterBuilder<T> setupOutpuFlusher(
		Function<Runnable, Future<?>> flushSubmit,
		long timeout) {
		this.timeout = timeout;
		if (flushSubmit != null) {
			this.flushSubmit = flushSubmit;
		}
		return this;
	}

	public RecordWriterBuilder<T> setTaskName(String taskName) {
		this.taskName = taskName;
		return this;
	}

	public RecordWriter<T> build(ResultPartitionWriter writer) {
		if (selector.isBroadcast()) {
			return new BroadcastRecordWriter(writer, flushSubmit, timeout, taskName);
		} else {
			return new ChannelSelectorRecordWriter<>(writer, selector, flushSubmit, timeout, taskName);
		}
	}

	private Future<?> runLocally(Runnable runnable) {
		runnable.run();
		return CompletableFuture.completedFuture(null);
	}
}
