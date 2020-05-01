package io.palyvos.provenance.usecases.cars.cloud.noprovenance;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class CarCloudAverageSpeed implements
    AggregateFunction<Tuple3<Integer, Double, Long>, Tuple3<Double, Integer, Long>, Tuple2<Double, Long>> {

  @Override
  public Tuple3<Double, Integer, Long> createAccumulator() {
    return new Tuple3<Double, Integer, Long>(0.0, 0, 0L);
  }

  @Override
  public Tuple3<Double, Integer, Long> add(
      Tuple3<Integer, Double, Long> value, Tuple3<Double, Integer, Long> accumulator) {
    // f0 -> sum
    // f1 -> count
    accumulator.f0 += value.f1;
    accumulator.f1 += 1;
    // pass on the stimulus
    accumulator.f2 = Math.max(value.f2, accumulator.f2);
    return accumulator;
  }

  @Override
  public Tuple2<Double, Long> getResult(Tuple3<Double, Integer, Long> accumulator) {
    return Tuple2
        .of(accumulator.f0 / accumulator.f1, accumulator.f2); // average = sum/count
  }

  @Override
  public Tuple3<Double, Integer, Long> merge(
      Tuple3<Double, Integer, Long> a, Tuple3<Double, Integer, Long> b) {
    throw new UnsupportedOperationException("merge");
  }
}
