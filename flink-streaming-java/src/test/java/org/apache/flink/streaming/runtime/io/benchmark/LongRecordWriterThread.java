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

package org.apache.flink.streaming.runtime.io.benchmark;

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.types.LongValue;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Wrapping thread around {@link RecordWriter} that sends a fixed number of <tt>LongValue(0)</tt>
 * records.
 */
public class LongRecordWriterThread extends CheckedThread {
	private final RecordWriter<LongValue> recordWriter;
	private final boolean broadcastMode;

	private final MailboxProcessor mailboxProcessor;

	/**
	 * Future to wait on a definition of the number of records to send.
	 */
	private CompletableFuture<Long> recordsToSend = new CompletableFuture<>();

	private long currentRecordIteration = 0;
	private long recordIterationLimit = -1;

	public LongRecordWriterThread(
		RecordWriterBuilder<LongValue> recordWriterBuilder,
		ResultPartitionWriter resultPartitionWriter,
		int flushTimeout, boolean broadcastMode) {
		this.broadcastMode = broadcastMode;

		mailboxProcessor = new MailboxProcessor(this::defaultAction);
		recordWriter = recordWriterBuilder
			.setupOutpuFlusher(
				runnable -> mailboxProcessor.getMainMailboxExecutor().submit(runnable, "OutputFluhser"),
				flushTimeout)
			.build(resultPartitionWriter);
	}

	public synchronized void shutdown() {
		mailboxProcessor.close();
		recordsToSend.complete(0L);
	}

	/**
	 * Initializes the record writer thread with this many numbers to send.
	 *
	 * <p>If the thread was already started, if may now continue.
	 *
	 * @param records
	 * 		number of records to send
	 */
	public synchronized void setRecordsToSend(long records) {
		checkState(!recordsToSend.isDone());
		recordsToSend.complete(records);
	}

	private synchronized CompletableFuture<Long> getRecordsToSend() {
		return recordsToSend;
	}

	private synchronized void finishSendingRecords() {
		recordsToSend = new CompletableFuture<>();
	}

	private void defaultAction(MailboxDefaultAction.Controller controller) {
		try {
			sendRecords();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void go() throws Exception {
		try {
			mailboxProcessor.runMailboxLoop();
		}
		finally {
			recordWriter.close();
		}
	}

	private void sendRecords() throws Exception {
		LongValue value = new LongValue(0);

		if (recordIterationLimit < 0) {
			recordIterationLimit = getRecordsToSend().get();
			currentRecordIteration = 1;
		}
		else if (currentRecordIteration++ < recordIterationLimit) {
			if (broadcastMode) {
				recordWriter.broadcastEmit(value);
			}
			else {
				recordWriter.emit(value);
			}
		}
		else {
			value.setValue(recordIterationLimit);
			recordWriter.broadcastEmit(value);
			recordWriter.flushAll();
		}
		finishSendingRecords();
	}
}
