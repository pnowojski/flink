package org.apache.flink.streaming.runtime.io.benchmark;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;

public class UserPojo extends Tuple4<Long, Integer, Long, byte[]> {

	private static final long serialVersionUID = -1625207099642006860L;

	public UserPojo(Long sequence, Integer id, Long timestamp, byte[] payload) {
		super(sequence, id, timestamp, payload);
	}

	public UserPojo() {}

	public UserPojo(long sequence)
	{
		this(sequence, (int) (sequence % Integer.MAX_VALUE), sequence, new byte[12]);
	}

	public UserPojo copy() {
		return new UserPojo(this.f0, this.f1, this.f2, this.f3);
	}

	public static TupleSerializer<UserPojo> getSerializer() {
		TypeSerializer<?>[] fieldSerializers = {
			LongSerializer.INSTANCE,
			IntSerializer.INSTANCE,
			LongSerializer.INSTANCE,
			BytePrimitiveArraySerializer.INSTANCE
		};
		return new TupleSerializer<>(UserPojo.class, fieldSerializers);
	}
}
