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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TestingCheckpointRecoveryFactory;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.state.AbstractSnapshotStrategy;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * Integrated tests to verify the logic to notify checkpoint aborted via RPC message.
 */
@RunWith(Parameterized.class)
public class NotifyCheckpointAbortedITCase extends TestLogger {
	private static final long SKIPPED_CHECKPOINT_ID = 1L;
	private static final long ABORTED_CHECKPOINT_ID = 2L;
	private static final long TEST_TIMEOUT = 60000;
	private static final String StuckAsyncCheckpointMapName = "StuckAsyncCheckpointMap";
	private static MiniClusterWithClientResource cluster;

	private static Path checkpointPath;
	private static String localRecoveryFolder;
	private static StuckAsyncSnapshotStrategy stuckAsyncSnapshotStrategy;

	@Parameterized.Parameter
	public boolean unalignedCheckpointEnabled;

	@Parameterized.Parameters(name = "unalignedCheckpointEnabled ={0}")
	public static Collection<Boolean> parameter() {
		return Arrays.asList(true, false);
	}

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Before
	public void setup() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, true);
		localRecoveryFolder = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
		configuration.setString(CheckpointingOptions.LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS, localRecoveryFolder);
		configuration.setString(HighAvailabilityOptions.HA_MODE, TestingHAFactory.class.getName());

		checkpointPath = new Path(TEMPORARY_FOLDER.newFolder().toURI());
		cluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(configuration)
				.setNumberTaskManagers(1)
				.setNumberSlotsPerTaskManager(1).build());
		cluster.before();

		stuckAsyncSnapshotStrategy = new StuckAsyncSnapshotStrategy();
		TestingCompletedCheckpointStore.addCheckpointLatch.reset();
		TestingCompletedCheckpointStore.abortCheckpointLatch.reset();
		StuckAsyncCheckpointMap.checkpointAbortedLatch.reset();
		BeforeExecuteCheckpointSink.notifiedCheckpointLatch.reset();
		BeforeExecuteCheckpointSink.snapshotIds.clear();
	}

	@After
	public void shutdown() {
		if (cluster != null) {
			cluster.after();
			cluster = null;
		}

	}

	/**
	 * Verify operator at different phase of checkpoint could act as expected when notified of checkpoint abortion.
	 *
	 * <p>The job would run with at least two checkpoints. The 1st checkpoint would fail to add checkpoint to store,
	 * and we verify all local states stored in 1st checkpoint would then discarded. The 2nd checkpoint would decline
	 * by 'DeclineSink'. Then we verify the async runnable future of 'StuckAsyncCheckpointMap' was canceled as expected,
	 * and 'BeforeExecuteCheckpointSink' did not execute the sync phase of checkpoint.
	 *
	 * <p>The job graph looks like:
	 * NormalSource --> keyBy --> NormalMap --> StuckAsyncCheckpointMap --> DeclineSink
	 *                                  |
	 *                                  |--> BarrierDelayMap -> BeforeExecuteCheckpointSink
	 */
	@Test(timeout = TEST_TIMEOUT)
	public void testNotifyCheckpointAborted() throws Exception {
		Duration timeout = Duration.ofMillis(TEST_TIMEOUT);
		Deadline deadline = Deadline.now().plus(timeout);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().enableUnalignedCheckpoints(unalignedCheckpointEnabled);
		env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);
		env.disableOperatorChaining();
		env.setParallelism(1);

		final StateBackend notifiedStateBackend = new NotifiedStateBackend(checkpointPath);
		env.setStateBackend(notifiedStateBackend);

		SingleOutputStreamOperator<Integer> normalMapStream = env
			.addSource(new NormalSource()).name("NormalSource")
			.keyBy((KeySelector<Tuple2<Integer, Integer>, Integer>) value -> value.f0)
			.map(new NormalMapFunction()).name("NormalMap");

		normalMapStream.
			transform(StuckAsyncCheckpointMapName, TypeInformation.of(Integer.class), new StuckAsyncCheckpointMap())
			.addSink(new DeclineSink()).name("DeclineSink");

		normalMapStream
			.transform("BarrierDelayMap", TypeInformation.of(Integer.class), new BarrierDelayMap())
			.transform("BeforeExecuteCheckpointSink", TypeInformation.of(Object.class), new BeforeExecuteCheckpointSink());

		final ClusterClient<?> clusterClient = cluster.getClusterClient();
		JobGraph jobGraph = env.getStreamGraph().getJobGraph();
		JobID jobID = jobGraph.getJobID();

		ClientUtils.submitJob(clusterClient, jobGraph);

		TestingCompletedCheckpointStore.addCheckpointLatch.await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

		Set<File> localStoredStates = new HashSet<>();
		while (deadline.hasTimeLeft()) {
			// found the local state manager has stored states for checkpoint-1.
			localStoredStates.addAll(collectLocalStoredStates());
			if (!localStoredStates.isEmpty()) {
				break;
			}
		}

		// let the checkpoint-1 failed finally.
		TestingCompletedCheckpointStore.abortCheckpointLatch.trigger();
		while (deadline.hasTimeLeft()) {
			// verify the local state manager has been cleaned up once notified as checkpoint-1 aborted.
			localStoredStates.removeIf(file -> !file.exists());
			if (localStoredStates.isEmpty()) {
				break;
			}
		}

		// wait for StuckAsyncCheckpointMap notified as checkpoint aborted.
		StuckAsyncCheckpointMap.checkpointAbortedLatch.await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
		assertTrue(stuckAsyncSnapshotStrategy.blockingRunnableFuture.isCancelled());

		// verify BeforeExecuteCheckpointSink never execute checkpoint-1.
		while (deadline.hasTimeLeft()) {
			if (!BeforeExecuteCheckpointSink.snapshotIds.isEmpty()) {
				if (!BeforeExecuteCheckpointSink.snapshotIds.contains(ABORTED_CHECKPOINT_ID)) {
					break;
				}
			}
		}

		clusterClient.cancel(jobID).get();
	}

	private Set<File> collectLocalStoredStates() throws IOException {
		return Files.find(Paths.get(localRecoveryFolder), Integer.MAX_VALUE, (filePath, fileAttr) -> fileAttr.isRegularFile()).map(java.nio.file.Path::toFile).collect(Collectors.toSet());
	}

	/**
	 * Normal source function.
	 */
	private static class NormalSource implements SourceFunction<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;
		protected volatile boolean running;

		NormalSource() {
			this.running = true;
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(Tuple2.of(ThreadLocalRandom.current().nextInt(), ThreadLocalRandom.current().nextInt()));
				}
				Thread.sleep(10);
			}
		}

		@Override
		public void cancel() {
			this.running = false;
		}
	}

	/**
	 * Normal map function.
	 */
	private static class NormalMapFunction implements MapFunction<Tuple2<Integer, Integer>, Integer>, CheckpointedFunction {
		private static final long serialVersionUID = 1L;
		private ValueState<Integer> valueState;

		@Override
		public Integer map(Tuple2<Integer, Integer> value) throws Exception {
			valueState.update(value.f1);
			return value.f1;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) {
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			valueState = context.getKeyedStateStore().getState(new ValueStateDescriptor<>("value", Integer.class));
		}
	}

	/**
	 * This map operator would stuck in async phase of checkpoint of `ABORTED_CHECKPOINT_ID`.
	 */
	private static class StuckAsyncCheckpointMap extends StreamMap<Integer, Integer> {
		private static final long serialVersionUID = 1L;
		private static final OneShotLatch checkpointAbortedLatch = new OneShotLatch();

		public StuckAsyncCheckpointMap() {
			super((MapFunction<Integer, Integer>) value -> value);
		}

		@Override
		public void notifyCheckpointAborted(long checkpointId) {
			if (operatorPlayItsRole(checkpointId)) {
				checkpointAbortedLatch.trigger();
			}
		}
	}

	/**
	 * Decline sink which would decline at checkpoint of `ABORTED_CHECKPOINT_ID`.
	 */
	private static class DeclineSink implements SinkFunction<Integer>, CheckpointedFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			if (operatorPlayItsRole(context.getCheckpointId())) {
				throw new ExpectedTestException();
			}
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			// do nothing.
		}
	}

	/**
	 * The map operator would delay to send barrier to downstream at checkpoint of `ABORTED_CHECKPOINT_ID`.
	 * This operator let {@link BeforeExecuteCheckpointSink} not enter checkpoint barrier align phase.
	 */
	private static class BarrierDelayMap extends StreamMap<Integer, Integer> {
		private static final long serialVersionUID = 1L;

		public BarrierDelayMap() {
			super((MapFunction<Integer, Integer>) value -> value);
		}

		@Override
		public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
			if (operatorPlayItsRole(checkpointId)) {
				BeforeExecuteCheckpointSink.notifiedCheckpointLatch.await();
			}
			super.prepareSnapshotPreBarrier(checkpointId);
		}
	}

	/**
	 * The sink which would receive barrier later at checkpoint of `ABORTED_CHECKPOINT_ID`, thus we could verify
	 * checkpoint would not execute in sync phase after notification.
	 */
	private static class BeforeExecuteCheckpointSink extends StreamSink<Integer> {
		private static final long serialVersionUID = 1L;
		private static final OneShotLatch notifiedCheckpointLatch = new OneShotLatch();
		private static final Set<Long> snapshotIds = new ConcurrentSkipListSet<>();

		public BeforeExecuteCheckpointSink() {
			super(new SinkFunction<Integer>() {
				private static final long serialVersionUID = 1L;
			});
		}

		@Override
		public void notifyCheckpointAborted(long checkpointId) {
			if (operatorPlayItsRole(checkpointId)) {
				notifiedCheckpointLatch.trigger();
			}
		}

		@Override
		public void snapshotState(StateSnapshotContext context) throws Exception {
			if (context.getCheckpointId() != SKIPPED_CHECKPOINT_ID) {
				snapshotIds.add(context.getCheckpointId());
			}
		}
	}

	/**
	 * The state backend to create {@link StuckAsyncOperatorStateBackend}.
	 */
	private static class NotifiedStateBackend extends FsStateBackend {
		private static final long serialVersionUID = 1L;

		public NotifiedStateBackend(Path checkpointDataUri) {
			super(checkpointDataUri);
		}

		@Override
		public NotifiedStateBackend configure(ReadableConfig config, ClassLoader classLoader) {
			return new NotifiedStateBackend(checkpointPath);
		}

		@Override
		public OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier, @Nonnull Collection<OperatorStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) throws BackendBuildingException {
			if (operatorIdentifier.contains(StuckAsyncCheckpointMapName)) {
				return new StuckAsyncOperatorStateBackend(
					env.getExecutionConfig(),
					cancelStreamRegistry,
					stuckAsyncSnapshotStrategy);
			} else {
				return new DefaultOperatorStateBackendBuilder(
					env.getUserClassLoader(),
					env.getExecutionConfig(),
					false,
					stateHandles,
					cancelStreamRegistry).build();
			}
		}
	}

	/**
	 * The operator statebackend to create {@link StuckAsyncSnapshotStrategy}.
	 */
	private static class StuckAsyncOperatorStateBackend extends DefaultOperatorStateBackend {

		public StuckAsyncOperatorStateBackend(
			ExecutionConfig executionConfig,
			CloseableRegistry closeStreamOnCancelRegistry,
			AbstractSnapshotStrategy<OperatorStateHandle> snapshotStrategy) {
			super(executionConfig,
				closeStreamOnCancelRegistry,
				new HashMap<>(),
				new HashMap<>(),
				new HashMap<>(),
				new HashMap<>(),
				snapshotStrategy);
		}
	}

	/**
	 * The snapshot strategy to create {@link BlockingRunnableFuture}.
	 */
	private static class StuckAsyncSnapshotStrategy extends AbstractSnapshotStrategy<OperatorStateHandle> {
		private BlockingRunnableFuture blockingRunnableFuture;

		protected StuckAsyncSnapshotStrategy() {
			super("StuckAsyncSnapshotStrategy");
			this.blockingRunnableFuture = new BlockingRunnableFuture();
		}

		@Override
		public RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot(long checkpointId, long timestamp, @Nonnull CheckpointStreamFactory streamFactory, @Nonnull CheckpointOptions checkpointOptions) {
			if (operatorPlayItsRole(checkpointId)) {
				return blockingRunnableFuture;
			} else {
				return DoneFuture.of(SnapshotResult.empty());
			}
		}
	}

	/**
	 * A blocking runnable future.
	 */
	private static final class BlockingRunnableFuture implements RunnableFuture<SnapshotResult<OperatorStateHandle>> {
		private final CompletableFuture<SnapshotResult<OperatorStateHandle>> future = new CompletableFuture<>();
		private final CountDownLatch countDownLatch;

		private BlockingRunnableFuture() {
			// count down twice to wait for notify checkpoint aborted to cancel.
			this.countDownLatch = new CountDownLatch(2);
		}

		@Override
		public void run() {
			countDownLatch.countDown();

			try {
				countDownLatch.await();
			} catch (InterruptedException e) {
				ExceptionUtils.rethrow(e);
			}

			future.complete(SnapshotResult.empty());
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			future.cancel(mayInterruptIfRunning);
			return true;
		}

		@Override
		public boolean isCancelled() {
			return future.isCancelled();
		}

		@Override
		public boolean isDone() {
			return future.isDone();
		}

		@Override
		public SnapshotResult<OperatorStateHandle> get() throws InterruptedException, ExecutionException {
			return future.get();
		}

		@Override
		public SnapshotResult<OperatorStateHandle> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
			return future.get();
		}
	}

	private static boolean operatorPlayItsRole(long checkpointId) {
		return checkpointId == ABORTED_CHECKPOINT_ID;
	}

	private static class TestingHaServices extends EmbeddedHaServices {
		private final CheckpointRecoveryFactory checkpointRecoveryFactory;

		TestingHaServices(CheckpointRecoveryFactory checkpointRecoveryFactory, Executor executor) {
			super(executor);
			this.checkpointRecoveryFactory = checkpointRecoveryFactory;
		}

		@Override
		public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
			return checkpointRecoveryFactory;
		}
	}

	/**
	 * An extension of {@link StandaloneCompletedCheckpointStore}.
	 */
	private static class TestingCompletedCheckpointStore extends StandaloneCompletedCheckpointStore {
		private static final OneShotLatch addCheckpointLatch = new OneShotLatch();
		private static final OneShotLatch abortCheckpointLatch = new OneShotLatch();

		TestingCompletedCheckpointStore() {
			super(1);
		}

		@Override
		public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
			if (abortCheckpointLatch.isTriggered()) {
				super.addCheckpoint(checkpoint);
			} else {
				// tell main thread that all checkpoints on task side have been finished.
				addCheckpointLatch.trigger();
				// wait for the main thread to throw exception so that the checkpoint would be notified as aborted.
				abortCheckpointLatch.await();
				throw new ExpectedTestException();
			}
		}
	}

	/**
	 * Testing HA factory which needs to be public in order to be instantiatable.
	 */
	public static class TestingHAFactory implements HighAvailabilityServicesFactory {

		@Override
		public HighAvailabilityServices createHAServices(Configuration configuration, Executor executor) {
			return new TestingHaServices(
				new TestingCheckpointRecoveryFactory(new TestingCompletedCheckpointStore(), new StandaloneCheckpointIDCounter()),
				executor);
		}
	}

}
