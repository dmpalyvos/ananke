package io.palyvos.provenance.usecases.cars.cloud.provenance;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogDataSerializer;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import java.io.Serializable;

public class CarCloudInputTupleGL implements Serializable, GenealogTuple {

  public Long f0;
  public Integer f1;
  public Double f2;
  public Double f3;
  private long stimulus;
  private GenealogData genealogData;

  public static CarCloudInputTupleGL newEmptyInstance() {
    CarCloudInputTupleGL tuple = new CarCloudInputTupleGL(0L, 0, -1.0, -1.0, 0);
    tuple.initGenealog(GenealogTupleType.SOURCE);
    return tuple;
  }

  public CarCloudInputTupleGL(Long value0, Integer value1, Double value2, Double value3, long stimulus) {
    this.f0 = value0;
    this.f1 = value1;
    this.f2 = value2;
    this.f3 = value3;
    this.stimulus = stimulus;
  }

  public CarCloudInputTupleGL(Long value0, Integer value1, Double value2, Double value3) {
    this(value0, value1, value2, value3, System.currentTimeMillis());
  }


  @Override
  public long getTimestamp() {
    return this.f0;
  }

  @Override
  public void setTimestamp(long timestamp) {
    this.f0 = timestamp;
  }

  public long getStimulus() {
    return stimulus;
  }

  @Override
  public void setStimulus(long stimulus) {
    this.stimulus = stimulus;
  }

  @Override
  public void initGenealog(GenealogTupleType tupleType) {
    this.genealogData = new GenealogData();
    genealogData.init(tupleType);
  }

  @Override
  public GenealogData getGenealogData() {
    return genealogData;
  }

  public void setGenealogData(GenealogData genealogData) {
    this.genealogData = genealogData;
  }

  public static class KryoSerializer extends Serializer<CarCloudInputTupleGL> implements
      Serializable {

    private final GenealogDataSerializer genealogDataSerializer;

    public KryoSerializer(
        GenealogDataSerializer genealogDataSerializer) {
      this.genealogDataSerializer = genealogDataSerializer;
    }

    @Override
    public void write(Kryo kryo, Output output, CarCloudInputTupleGL object) {
      output.writeLong(object.f0);
      output.writeInt(object.f1);
      output.writeDouble(object.f2);
      output.writeDouble(object.f3);
      output.writeLong(object.stimulus);
      genealogDataSerializer.write(kryo, output, object.genealogData);
    }

    @Override
    public CarCloudInputTupleGL read(Kryo kryo, Input input, Class<CarCloudInputTupleGL> type) {
      CarCloudInputTupleGL tuple = new CarCloudInputTupleGL(input.readLong(), input.readInt(),
          input.readDouble(), input.readDouble());
      tuple.stimulus = input.readLong();
      tuple.genealogData = genealogDataSerializer.read(kryo, input, GenealogData.class);
      return tuple;
    }
  }

  @Override
  public String toString() {
    return String.format("%d,%d,%f,%f", f0, f1, f2, f3);
  }
}
