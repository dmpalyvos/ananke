package io.palyvos.provenance.usecases.cars.local;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;

public class CarLocalInputTuple implements Serializable {

  private long stimulus;

  public long f0;
  public LidarImageContainer f1;
  public LidarImageContainer f2;
  public LidarImageContainer f3;

  public CarLocalInputTuple() {

  }

  public CarLocalInputTuple(Long value0, LidarImageContainer value1, LidarImageContainer value2,
      LidarImageContainer value3) {
    this.f0 = value0;
    this.f1 = value1;
    this.f2 = value2;
    this.f3 = value3;
    this.stimulus = System.currentTimeMillis();
  }

  public CarLocalInputTuple(Long value0, LidarImageContainer value1, LidarImageContainer value2,
      LidarImageContainer value3, Long stimulus) {
    this.f0 = value0;
    this.f1 = value1;
    this.f2 = value2;
    this.f3 = value3;
    this.stimulus = stimulus;
  }


  public long getStimulus() {
    return stimulus;
  }

  public static class KryoSerializer extends Serializer<CarLocalInputTuple> implements
      Serializable {

    private final LidarImageContainer.KryoSerializer lidarContainerSerializer = new LidarImageContainer.KryoSerializer();

    @Override
    public void write(Kryo kryo, Output output, CarLocalInputTuple object) {
      output.writeLong(object.f0);
      lidarContainerSerializer.write(kryo, output, object.f1);
      lidarContainerSerializer.write(kryo, output, object.f2);
      lidarContainerSerializer.write(kryo, output, object.f3);
      output.writeLong(object.getStimulus());
    }

    @Override
    public CarLocalInputTuple read(Kryo kryo, Input input, Class<CarLocalInputTuple> type) {
      return new CarLocalInputTuple(input.readLong(),
          lidarContainerSerializer.read(kryo, input, LidarImageContainer.class),
          lidarContainerSerializer.read(kryo, input, LidarImageContainer.class),
          lidarContainerSerializer.read(kryo, input, LidarImageContainer.class),
          input.readLong());
    }
  }

  @Override
  public String toString() {
    return String.format("%d,%s,%s,%s", f0, f1, f2, f3);
  }
}
