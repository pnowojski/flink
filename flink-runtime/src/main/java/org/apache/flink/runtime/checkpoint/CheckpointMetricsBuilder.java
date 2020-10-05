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

package org.apache.flink.runtime.checkpoint;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A builder for {@link CheckpointMetrics}.
 *
 * <p>This class is not thread safe, but parts of it can actually be used from different threads.
 */
@NotThreadSafe
public class CheckpointMetricsBuilder {
	private long alignmentDurationNanos = -1L;
	private long syncDurationMillis = -1L;
	private long asyncDurationMillis = -1L;
	private long checkpointStartDelayNanos = -1L;

	public CheckpointMetricsBuilder setAlignmentDurationNanos(long alignmentDurationNanos) {
		this.alignmentDurationNanos = alignmentDurationNanos;
		return this;
	}

	public long getAlignmentDurationNanos() {
		return alignmentDurationNanos;
	}

	public CheckpointMetricsBuilder setSyncDurationMillis(long syncDurationMillis) {
		this.syncDurationMillis = syncDurationMillis;
		return this;
	}

	public long getSyncDurationMillis() {
		return syncDurationMillis;
	}

	public CheckpointMetricsBuilder setAsyncDurationMillis(long asyncDurationMillis) {
		this.asyncDurationMillis = asyncDurationMillis;
		return this;
	}

	public long getAsyncDurationMillis() {
		return asyncDurationMillis;
	}

	public CheckpointMetricsBuilder setCheckpointStartDelayNanos(long checkpointStartDelayNanos) {
		this.checkpointStartDelayNanos = checkpointStartDelayNanos;
		return this;
	}

	public long getCheckpointStartDelayNanos() {
		return checkpointStartDelayNanos;
	}

	public CheckpointMetrics build() {
		return new CheckpointMetrics(
			alignmentDurationNanos,
			syncDurationMillis,
			asyncDurationMillis,
			checkpointStartDelayNanos);
	}
}
