package io.palyvos.provenance.usecases;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.util.BaseTuple;
import java.io.Serializable;
import java.util.Objects;

public class CountTuple extends BaseTuple {

  protected final long count;

  public CountTuple(long timestamp, String key, long stimulus, long count) {
    super(timestamp, key, stimulus);
    this.count = count;
  }

  public long getCount() {
    return count;
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
    CountTuple that = (CountTuple) o;
    return count == that.count;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), count);
  }

  @Override
  public String toString() {
    return getTimestamp() + "," + getKey() + "," + getCount();
  }

  public static class KryoSerializer extends Serializer<CountTuple> implements Serializable {

    @Override
    public void write(Kryo kryo, Output output, CountTuple object) {
      output.writeLong(object.getTimestamp());
      output.writeString(object.getKey());
      output.writeLong(object.getStimulus());
      output.writeLong(object.getCount());
    }

    @Override
    public CountTuple read(Kryo kryo, Input input, Class<CountTuple> type) {
      return new CountTuple(
          input.readLong(), input.readString(), input.readLong(), input.readLong());
    }
  }
}
