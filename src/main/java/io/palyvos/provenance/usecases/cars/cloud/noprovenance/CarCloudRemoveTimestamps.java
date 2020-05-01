package io.palyvos.provenance.usecases.cars.cloud.noprovenance;

import io.palyvos.provenance.usecases.cars.cloud.CarCloudInputTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class CarCloudRemoveTimestamps implements
    MapFunction<CarCloudInputTuple, Tuple4<Integer, Double, Double , Long>> {

  @Override
  public Tuple4<Integer, Double, Double, Long> map(CarCloudInputTuple t)
      throws Exception {
    return Tuple4.of(t.f1, t.f2, t.f3, t.getStimulus());
  }
}
