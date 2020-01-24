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
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.types.LongValue;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Wrapping thread around {@link RecordWriter} that sends a fixed number of <tt>LongValue(0)</tt>
 * records.
 */
@ThreadSafe
public class LongRecordWriterThread extends CheckedThread {
	private final RecordWriter<LongValue> recordWriter;
	private final boolean broadcastMode;

	private final MailboxProcessor mailboxProcessor;
	private final MailboxExecutor mailboxExecutor;

	private long currentRecordIteration = 0;
	private long recordIterationLimit = -1;

	@Nullable
	private MailboxDefaultAction.Suspension suspension;

	public LongRecordWriterThread(
			RecordWriter<LongValue> recordWriter,
			boolean broadcastMode) {
		this.broadcastMode = broadcastMode;

		mailboxProcessor = new MailboxProcessor(
			this::defaultAction,
			new TaskMailboxImpl(this),
			StreamTaskActionExecutor.IMMEDIATE);
		mailboxExecutor = mailboxProcessor.getMainMailboxExecutor();
		this.recordWriter = recordWriter;
	}

	public void shutdown() {
		mailboxProcessor.allActionsCompleted();
	}

	/**
	 * Initializes the record writer thread with this many numbers to send.
	 *
	 * <p>If the thread was already started, if may now continue.
	 *
	 * @param records
	 * 		number of records to send
	 */
	public void setRecordsToSend(long records) {
		mailboxExecutor.execute(() -> resumeAndSetRecordsToSend(records), "resumeAndSetRecordsToSend");
	}

	private void resumeAndSetRecordsToSend(long records) {
		recordIterationLimit = records;
		currentRecordIteration = 1;

		if (suspension != null) {
			suspension.resume();
		}
	}

	private void defaultAction(MailboxDefaultAction.Controller controller) {
		try {
			if (recordIterationLimit < 0) {
				waitForRecordsToSend(controller);
			}
			sendRecords(controller);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void go() throws Exception {
		try {
			mailboxProcessor.runMailboxLoop();
		}
		catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		finally {
			recordWriter.close();
		}
	}

	private void sendRecords(MailboxDefaultAction.Controller controller) throws Exception {
		LongValue value = new LongValue(0);

		if (currentRecordIteration++ < recordIterationLimit) {
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

			waitForRecordsToSend(controller);
		}
	}

	private void waitForRecordsToSend(MailboxDefaultAction.Controller controller) {
		suspension = controller.suspendDefaultAction();
	}
}
