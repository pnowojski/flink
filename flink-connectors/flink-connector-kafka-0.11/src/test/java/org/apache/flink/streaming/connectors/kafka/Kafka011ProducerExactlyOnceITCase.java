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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

/**
 * IT cases for the {@link FlinkKafkaProducer011}.
 */
@SuppressWarnings("serial")
public class Kafka011ProducerExactlyOnceITCase extends KafkaProducerTestBase {
	@BeforeClass
	public static void prepare() throws ClassNotFoundException {
		KafkaProducerTestBase.prepare();
		((KafkaTestEnvironmentImpl) kafkaServer).setProducerSemantic(FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);
	}

	@Override
	public void testOneToOneAtLeastOnceRegularSink() throws Exception {
		// TODO: fix this test
		// currently very often (~50% cases) KafkaProducer live locks itself on commitTransaction call.
		// Somehow Kafka 0.11 doesn't play along with NetworkFailureProxy. This can either mean a bug in Kafka
		// that it doesn't work well with some weird network failures, or the NetworkFailureProxy is a broken design
		// and this test should be reimplemented in completely different way...
	}

	@Override
	public void testOneToOneAtLeastOnceCustomOperator() throws Exception {
		// TODO: fix this test
		// currently very often (~50% cases) KafkaProducer live locks itself on commitTransaction call.
		// Somehow Kafka 0.11 doesn't play along with NetworkFailureProxy. This can either mean a bug in Kafka
		// that it doesn't work well with some weird network failures, or the NetworkFailureProxy is a broken design
		// and this test should be reimplemented in completely different way...
	}

	@Test(timeout = 30000L)
	public void name() throws Exception {
		final String topic = "producer-test-" + System.currentTimeMillis();

		createTestTopic(topic, 1, 1);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setParallelism(32);
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();

		Properties properties = new Properties();
		properties.putAll(standardProps);
		properties.putAll(secureProps);
		// decrease timeout and block time from 60s down to 10s - this is how long KafkaProducer will try send pending (not flushed) data on close()
		properties.setProperty("timeout.ms", "10000");
		properties.setProperty("max.block.ms", "10000");
		// increase batch.size and linger.ms - this tells KafkaProducer to batch produced events instead of flushing them immediately
		properties.setProperty("batch.size", "10240000");
		properties.setProperty("linger.ms", "10000");

		final DataStreamSource<String> stream = env.fromCollection(new InfiniteStringIterator(), String.class);
		stream
			.addSink(new FlinkKafkaProducer011<>(
				"producer-ea-test-" + System.currentTimeMillis(),
				new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
				properties,
				FlinkKafkaProducer011.Semantic.EXACTLY_ONCE))
			.name("kafka-sink-" + System.currentTimeMillis());

		// execute program
		env.execute("Flink Streaming Java API Skeleton");

		deleteTestTopic(topic);
	}

	public static class InfiniteStringIterator implements Iterator<String>, Serializable {

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public String next() {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return UUID.randomUUID().toString();
		}

	}
}
