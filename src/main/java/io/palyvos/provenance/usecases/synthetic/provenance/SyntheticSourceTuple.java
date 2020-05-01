package io.palyvos.provenance.usecases.synthetic.provenance;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.util.BaseTuple;
import io.palyvos.provenance.util.IncreasingUIDGenerator;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public class SyntheticSourceTuple extends BaseTuple implements TimestampedUIDTuple {

  private long uid;
  private byte[] data; // Do NOT make public or add getter, otherwise flink will think it's a POJO
  static class DummySourceTupleSupplier implements Supplier<TimestampedUIDTuple>, Serializable {

    private final int size;

    public DummySourceTupleSupplier(int size) {
      this.size = size;
    }

    @Override
    public TimestampedUIDTuple get() {
      return new SyntheticSourceTuple(size);
    }
  }

  public static DummySourceTupleSupplier supplier(int size) {
    return new DummySourceTupleSupplier(size);
  }

  public SyntheticSourceTuple(int size) {
    this.data = new byte[size];
    ThreadLocalRandom.current().nextBytes(data);
  }

  public SyntheticSourceTuple() {
    //Needed for deserialization
  }

  @Override
  public long getUID() {
    return uid;
  }

  @Override
  public void setUID(long uid) {
    this.uid = uid;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    SyntheticSourceTuple that = (SyntheticSourceTuple) o;
    return uid == that.uid &&
        Arrays.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(super.hashCode(), uid);
    result = 31 * result + Arrays.hashCode(data);
    return result;
  }

  @Override
  public String toString() {
    return String.format("S-%d,%s", timestamp, IncreasingUIDGenerator.asString(getUID()));
  }

  public static class KryoSerializer extends Serializer<SyntheticSourceTuple> implements Serializable {

    @Override
    public void write(Kryo kryo, Output output, SyntheticSourceTuple syntheticSourceTuple) {
      output.writeLong(syntheticSourceTuple.uid);
      output.writeLong(syntheticSourceTuple.stimulus);
      output.writeLong(syntheticSourceTuple.timestamp);
      output.writeString(syntheticSourceTuple.key);
      output.writeInt(syntheticSourceTuple.data.length);
      output.writeBytes(syntheticSourceTuple.data);
    }

    @Override
    public SyntheticSourceTuple read(Kryo kryo, Input input, Class<SyntheticSourceTuple> aClass) {
      SyntheticSourceTuple tuple = new SyntheticSourceTuple();
      tuple.setUID(input.readLong());
      tuple.setStimulus(input.readLong());
      tuple.setTimestamp(input.readLong());
      tuple.setKey(input.readString());
      final int dataSize = input.readInt();
      tuple.data = input.readBytes(dataSize);
      return tuple;
    }
  }
}
