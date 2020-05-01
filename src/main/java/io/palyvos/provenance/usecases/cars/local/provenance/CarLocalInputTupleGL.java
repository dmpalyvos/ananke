package io.palyvos.provenance.usecases.cars.local.provenance;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogDataSerializer;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.usecases.cars.local.LidarImageContainer;
import java.io.Serializable;

public class CarLocalInputTupleGL implements Serializable, GenealogTuple {

  private long stimulus;

  public long f0;
  public LidarImageContainer f1;
  public LidarImageContainer f2;
  public LidarImageContainer f3;
  private GenealogData genealogData;

  public CarLocalInputTupleGL() {

  }

  public CarLocalInputTupleGL(Long value0, LidarImageContainer value1, LidarImageContainer value2,
      LidarImageContainer value3) {
    this.f0 = value0;
    this.f1 = value1;
    this.f2 = value2;
    this.f3 = value3;
    this.stimulus = System.currentTimeMillis();
  }

  public CarLocalInputTupleGL(Long value0, LidarImageContainer value1, LidarImageContainer value2,
      LidarImageContainer value3, Long stimulus) {
    this.f0 = value0;
    this.f1 = value1;
    this.f2 = value2;
    this.f3 = value3;
    this.stimulus = stimulus;
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
    this.genealogData.init(tupleType);
  }

  @Override
  public GenealogData getGenealogData() {
    return genealogData;
  }

  public void setGenealogData(GenealogData genealogData) {
    this.genealogData = genealogData;
  }

  public static class KryoSerializer extends Serializer<CarLocalInputTupleGL> implements
      Serializable {

    private final LidarImageContainer.KryoSerializer lidarContainerSerializer = new LidarImageContainer.KryoSerializer();
    private final GenealogDataSerializer genealogDataSerializer;

    public KryoSerializer(GenealogDataSerializer genealogDataSerializer) {
      this.genealogDataSerializer = genealogDataSerializer;
    }

    @Override
    public void write(Kryo kryo, Output output, CarLocalInputTupleGL object) {
      output.writeLong(object.f0);
      lidarContainerSerializer.write(kryo, output, object.f1);
      lidarContainerSerializer.write(kryo, output, object.f2);
      lidarContainerSerializer.write(kryo, output, object.f3);
      output.writeLong(object.getStimulus());
      genealogDataSerializer.write(kryo, output, object.genealogData);
    }

    @Override
    public CarLocalInputTupleGL read(Kryo kryo, Input input, Class<CarLocalInputTupleGL> type) {
      CarLocalInputTupleGL tuple = new CarLocalInputTupleGL(input.readLong(),
          lidarContainerSerializer.read(kryo, input, LidarImageContainer.class),
          lidarContainerSerializer.read(kryo, input, LidarImageContainer.class),
          lidarContainerSerializer.read(kryo, input, LidarImageContainer.class),
          input.readLong());
      tuple.setGenealogData(genealogDataSerializer.read(kryo, input, GenealogData.class));
      return tuple;
    }
  }

  @Override
  public String toString() {
    return String.format("%d,%s,%s,%s", f0, f1, f2, f3);
  }
}
