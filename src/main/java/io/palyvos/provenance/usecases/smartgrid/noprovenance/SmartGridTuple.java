package io.palyvos.provenance.usecases.smartgrid.noprovenance;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.util.BaseTuple;
import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Pattern;

/** Tuple with schema {@code (id, ts, stimulus, value)} */
public class SmartGridTuple extends BaseTuple {

  private static final Pattern DELIMITER_PATTERN = Pattern.compile(",");
  private long value;

  public static SmartGridTuple fromReading(String meterReading) {
    try {
      String[] readings = DELIMITER_PATTERN.split(meterReading.trim());
      return new SmartGridTuple(readings);
    } catch (Exception exception) {
      throw new IllegalArgumentException(
          String.format("Failed to parse reading: %s", meterReading), exception);
    }
  }


  protected SmartGridTuple(String[] readings) {
    this(
        readings[1],
        Long.valueOf(readings[0]),
        System.currentTimeMillis(),
        Long.valueOf(readings[2]));
  }

  public SmartGridTuple(String key, long timestamp, long stimulus, long value) {
    super(timestamp, key, stimulus);
    this.value = value;
  }

  public SmartGridTuple() {}

  public long getValue() {
    return value;
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
    SmartGridTuple that = (SmartGridTuple) o;
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), value);
  }

  @Override
  public String toString() {
    return String.format("%d,%s,%d", getTimestamp(), getKey(), value);
  }

  public static class KryoSerializer extends com.esotericsoftware.kryo.Serializer<SmartGridTuple>
      implements Serializable {

    @Override
    public void write(Kryo kryo, Output output, SmartGridTuple object) {
      output.writeString(object.getKey());
      output.writeLong(object.getTimestamp());
      output.writeLong(object.getStimulus());
      output.writeLong(object.getValue());
    }

    @Override
    public SmartGridTuple read(Kryo kryo, Input input, Class<SmartGridTuple> type) {
      return new SmartGridTuple(
          input.readString(), input.readLong(), input.readLong(), input.readLong());
    }
  }
}
