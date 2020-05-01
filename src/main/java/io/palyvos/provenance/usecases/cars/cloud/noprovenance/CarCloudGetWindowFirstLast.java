package io.palyvos.provenance.usecases.cars.cloud.noprovenance;

import io.palyvos.provenance.usecases.cars.cloud.CarCloudInputTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class CarCloudGetWindowFirstLast implements AggregateFunction<
    CarCloudInputTuple,
    Tuple2<CarCloudInputTuple, CarCloudInputTuple>,
    Tuple3<CarCloudInputTuple, CarCloudInputTuple, Long>> {

  @Override
  public Tuple2<CarCloudInputTuple, CarCloudInputTuple> createAccumulator() {
    return new Tuple2<>(CarCloudInputTuple.newEmptyInstance(),
        CarCloudInputTuple.newEmptyInstance());
  }

  @Override
  public Tuple2<CarCloudInputTuple, CarCloudInputTuple> add(
      CarCloudInputTuple value, Tuple2<CarCloudInputTuple, CarCloudInputTuple> accumulator) {
    if (accumulator.f0.f3 == -1.0) {
      accumulator.f0 = value;
    } else {
      accumulator.f1 = value;
    }
    return accumulator;
  }

  @Override
  public Tuple3<CarCloudInputTuple, CarCloudInputTuple, Long> getResult(
      Tuple2<CarCloudInputTuple, CarCloudInputTuple> accumulator) {
    return Tuple3.of(accumulator.f0, accumulator.f1,
        Math.max(accumulator.f0.getStimulus(), accumulator.f1.getStimulus()));
  }

  @Override
  public Tuple2<CarCloudInputTuple, CarCloudInputTuple> merge(
      Tuple2<CarCloudInputTuple, CarCloudInputTuple> a,
      Tuple2<CarCloudInputTuple, CarCloudInputTuple> b) {
    throw new UnsupportedOperationException("merge");
  }
}
