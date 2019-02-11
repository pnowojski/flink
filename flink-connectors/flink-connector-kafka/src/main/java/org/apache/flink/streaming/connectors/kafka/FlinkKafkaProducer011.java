package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.KafkaTransactionContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.KafkaTransactionState;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.NextTransactionalIdHint;

/**
 * Compatibility class to make migration from 0.11 connector to the universal one.
 *
 * <p>For more details check FLINK-11249 and the discussion in the pull requests.
 */
public class FlinkKafkaProducer011 {
	public static class NextTransactionalIdHintSerializer {
		public static final class NextTransactionalIdHintSerializerSnapshot extends SimpleTypeSerializerSnapshot<NextTransactionalIdHint> {
			public NextTransactionalIdHintSerializerSnapshot() {
				super(FlinkKafkaProducer.NextTransactionalIdHintSerializer::new);
			}
		}
	}

	public static class ContextStateSerializer {
		public static final class ContextStateSerializerSnapshot extends SimpleTypeSerializerSnapshot<KafkaTransactionContext> {
			public ContextStateSerializerSnapshot() {
				super(FlinkKafkaProducer.ContextStateSerializer::new);
			}
		}
	}

	public static class TransactionStateSerializer {
		public static final class TransactionStateSerializerSnapshot extends SimpleTypeSerializerSnapshot<KafkaTransactionState> {
			public TransactionStateSerializerSnapshot() {
				super(FlinkKafkaProducer.TransactionStateSerializer::new);
			}
		}
	}
}
