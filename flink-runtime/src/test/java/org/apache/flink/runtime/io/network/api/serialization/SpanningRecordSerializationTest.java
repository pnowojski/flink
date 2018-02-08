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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.serialization.types.LargeObjectType;
import org.apache.flink.testutils.serialization.types.IntType;
import org.apache.flink.testutils.serialization.types.SerializationTestType;
import org.apache.flink.testutils.serialization.types.SerializationTestTypeFactory;
import org.apache.flink.testutils.serialization.types.Util;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSingleBuffer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createBufferBuilder;

/**
 * Tests for the {@link SpillingAdaptiveSpanningRecordDeserializer} and {@link AdaptiveSpanningRecordDeserializer}.
 */
public class SpanningRecordSerializationTest {

	@Test
	public void testIntRecordsSpanningMultipleSegments() throws Exception {
		final int segmentSize = 1;
		final int numValues = 10;

		testNonSpillingDeserializer(Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
		testSpillingDeserializer(Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
	}

	@Test
	public void testIntRecordsWithAlignedBuffers () throws Exception {
		final int segmentSize = 64;
		final int numValues = 64;

		testNonSpillingDeserializer(Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
		testSpillingDeserializer(Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
	}

	@Test
	public void testIntRecordsWithUnalignedBuffers () throws Exception {
		final int segmentSize = 31;
		final int numValues = 248;

		testNonSpillingDeserializer(Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
		testSpillingDeserializer(Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
	}

	@Test
	public void testRandomRecords () throws Exception {
		final int segmentSize = 127;
		final int numValues = 10000;

		testNonSpillingDeserializer(Util.randomRecords(numValues), segmentSize);
		testSpillingDeserializer(Util.randomRecords(numValues), segmentSize);
	}

	@Test
	public void testHandleMixedLargeRecords() throws Exception {
		final int numValues = 99;
		final int segmentSize = 32 * 1024;

		List<SerializationTestType> originalRecords = new ArrayList<>((numValues + 1) / 2);
		LargeObjectType genLarge = new LargeObjectType();
		Random rnd = new Random();

		for (int i = 0; i < numValues; i++) {
			if (i % 2 == 0) {
				originalRecords.add(new IntType(42));
			} else {
				originalRecords.add(genLarge.getRandom(rnd));
			}
		}

		testNonSpillingDeserializer(originalRecords, segmentSize);
		testSpillingDeserializer(originalRecords, segmentSize);
	}

	// -----------------------------------------------------------------------------------------------------------------

	private void testNonSpillingDeserializer(Iterable<SerializationTestType> records, int segmentSize) throws Exception {
		RecordSerializer<SerializationTestType> serializer = new SpanningRecordSerializer<>();
		RecordDeserializer<SerializationTestType> deserializer = new AdaptiveSpanningRecordDeserializer<>();

		testSerializationRoundTrip(records, segmentSize, serializer, deserializer);
	}

	private void testSpillingDeserializer(Iterable<SerializationTestType> records, int segmentSize) throws Exception {
		RecordSerializer<SerializationTestType> serializer = new SpanningRecordSerializer<>();
		RecordDeserializer<SerializationTestType> deserializer =
			new SpillingAdaptiveSpanningRecordDeserializer<>(
				new String[]{System.getProperty("java.io.tmpdir")});

		testSerializationRoundTrip(records, segmentSize, serializer, deserializer);
	}

	/**
	 * Iterates over the provided records and tests whether {@link SpanningRecordSerializer} and {@link AdaptiveSpanningRecordDeserializer}
	 * interact as expected.
	 *
	 * <p>Only a single {@link MemorySegment} will be allocated.
	 *
	 * @param records records to test
	 * @param segmentSize size for the {@link MemorySegment}
	 */
	private static void testSerializationRoundTrip(
			Iterable<SerializationTestType> records,
			int segmentSize,
			RecordSerializer<SerializationTestType> serializer,
			RecordDeserializer<SerializationTestType> deserializer)
		throws Exception {
		final ArrayDeque<SerializationTestType> serializedRecords = new ArrayDeque<>();

		// -------------------------------------------------------------------------------------------------------------

		BufferBuilder bufferBuilder = createBufferBuilder(segmentSize);
		serializer.setNextBufferBuilder(bufferBuilder);

		int numRecords = 0;
		for (SerializationTestType record : records) {

			serializedRecords.add(record);

			numRecords++;

			// serialize record
			if (serializer.addRecord(record).isFullBuffer()) {
				// buffer is full => start deserializing
				deserializer.setNextBuffer(buildSingleBuffer(bufferBuilder));

				while (!serializedRecords.isEmpty()) {
					SerializationTestType expected = serializedRecords.poll();
					SerializationTestType actual = expected.getClass().newInstance();

					if (deserializer.getNextRecord(actual).isFullRecord()) {
						Assert.assertEquals(expected, actual);
						numRecords--;
					} else {
						serializedRecords.addFirst(expected);
						break;
					}
				}

				// move buffers as long as necessary (for long records)
				bufferBuilder = createBufferBuilder(segmentSize);
				serializer.clear();
				while (serializer.setNextBufferBuilder(bufferBuilder).isFullBuffer()) {
					deserializer.setNextBuffer(buildSingleBuffer(bufferBuilder));
					bufferBuilder = createBufferBuilder(segmentSize);
					serializer.clear();
				}
			}
		}

		// deserialize left over records
		deserializer.setNextBuffer(buildSingleBuffer(bufferBuilder));

		while (!serializedRecords.isEmpty()) {
			SerializationTestType expected = serializedRecords.poll();

			SerializationTestType actual = expected.getClass().newInstance();
			RecordDeserializer.DeserializationResult result = deserializer.getNextRecord(actual);

			Assert.assertTrue(result.isFullRecord());
			Assert.assertEquals(expected, actual);
			numRecords--;
		}

		// assert that all records have been serialized and deserialized
		Assert.assertEquals(0, numRecords);
		Assert.assertFalse(serializer.hasSerializedData());
		Assert.assertFalse(deserializer.hasUnfinishedData());
	}
}
