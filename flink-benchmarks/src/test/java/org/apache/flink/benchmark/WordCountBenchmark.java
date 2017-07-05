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

package org.apache.flink.benchmark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.util.jartestprogram.WordCountWithInnerClass;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * Integration test for triggering and resuming from savepoints.
 */

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 20)
@Measurement(iterations = 1000)
public class WordCountBenchmark {

	@Benchmark
	public void benchmarkCount(Context context) throws Exception {

		StreamExecutionEnvironment env = context.env;
		// set up the execution environment
		env.setParallelism(1);
		env.getConfig().disableSysoutLogging();
		DataStreamSource<Long> source = env.addSource(new LongSource(1000000L));

		source
			.map(new MapFunction<Long, Long>() {
				@Override
				public Long map(Long value) throws Exception {
					return value * 2;
				}
			})
			.windowAll(GlobalWindows.create())
			.reduce(new ReduceFunction<Long>() {
				@Override
				public Long reduce(Long value1, Long value2) throws Exception {
					return value1 + value2;
				}
			})
			.print();

		env.execute();
	}

	@State(Thread)
	public static class Context {

		public final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		private List<Integer> input;

		@Setup
		public void setUp() {

		}

		public List<Integer> getInput() {
			return input;
		}
	}

	public class LongSource extends RichParallelSourceFunction<Long> {

		private volatile boolean running = true;
		private long maxValue;

		public LongSource(long maxValue) {
			this.maxValue = maxValue;
		}

		@Override
		public void run(SourceFunction.SourceContext<Long> ctx) throws Exception {
			long counter = 0;

			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(counter);
					counter++;
					if (counter >= maxValue) {
						cancel();
					}
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
