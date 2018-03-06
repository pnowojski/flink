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

import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.ReusingDeserializationDelegate;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.LongValue;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link ReceiverThread} that deserialize incoming messages.
 */
public class SerializingLongReceiver extends ReceiverThread {

	private final MutableRecordReader<DeserializationDelegate<StreamElement>> reader;

	private final StreamElementSerializer<UserPojo> serializer;

	private final ReusingDeserializationDelegate<StreamElement> deserializationDelegate;

	@SuppressWarnings("WeakerAccess")
	public SerializingLongReceiver(InputGate inputGate, int expectedRepetitionsOfExpectedRecord) {
		super(expectedRepetitionsOfExpectedRecord);
		this.reader = new MutableRecordReader<>(
			inputGate,
			new String[]{
				EnvironmentInformation.getTemporaryFileDirectory()
			});

		this.serializer = new StreamElementSerializer<>(UserPojo.getSerializer());
		this.deserializationDelegate = new ReusingDeserializationDelegate<>(serializer);
		deserializationDelegate.setInstance(new StreamRecord<UserPojo>(new UserPojo(0)));
	}

	protected void readRecords(long lastExpectedRecord) throws Exception {
		LOG.debug("readRecords(lastExpectedRecord = {})", lastExpectedRecord);
		final LongValue value = new LongValue();

		while (running && reader.next(deserializationDelegate)) {
			StreamElement instance = deserializationDelegate.getInstance();
			checkState(instance.isRecord());
			StreamRecord<UserPojo> record = instance.asRecord();
			value.setValue(record.getValue().f0);
			final long ts = value.getValue();
			if (ts == lastExpectedRecord) {
				expectedRecordCounter++;
				if (expectedRecordCounter == expectedRepetitionsOfExpectedRecord) {
					break;
				}
			}
		}
	}
}
