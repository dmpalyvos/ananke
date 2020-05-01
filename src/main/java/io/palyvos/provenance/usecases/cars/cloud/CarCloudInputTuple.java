package io.palyvos.provenance.usecases.cars.cloud;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;

public class CarCloudInputTuple implements Serializable {

  public Long f0;
  public Integer f1;
  public Double f2;
  public Double f3;
  private long stimulus;

  public static CarCloudInputTuple newEmptyInstance() {
    return new CarCloudInputTuple(0L, 0, -1.0, -1.0, 0);
  }

  public CarCloudInputTuple(Long value0, Integer value1, Double value2, Double value3, long stimulus) {
    this.f0 = value0;
    this.f1 = value1;
    this.f2 = value2;
    this.f3 = value3;
    this.stimulus = stimulus;
  }

  public CarCloudInputTuple(Long value0, Integer value1, Double value2, Double value3) {
    this(value0, value1, value2, value3, System.currentTimeMillis());
  }


  public long getStimulus() {
    return stimulus;
  }

  public static class KryoSerializer extends Serializer<CarCloudInputTuple> implements
      Serializable {

    @Override
    public void write(Kryo kryo, Output output, CarCloudInputTuple object) {
      output.writeLong(object.f0);
      output.writeInt(object.f1);
      output.writeDouble(object.f2);
      output.writeDouble(object.f3);
      output.writeLong(object.stimulus);
    }

    @Override
    public CarCloudInputTuple read(Kryo kryo, Input input, Class<CarCloudInputTuple> type) {
      CarCloudInputTuple tuple = new CarCloudInputTuple(input.readLong(), input.readInt(),
          input.readDouble(), input.readDouble());
      tuple.stimulus = input.readLong();
      return tuple;
    }
  }

  @Override
  public String toString() {
    return String.format("%d,%d,%f,%f", f0, f1, f2, f3);
  }
}
