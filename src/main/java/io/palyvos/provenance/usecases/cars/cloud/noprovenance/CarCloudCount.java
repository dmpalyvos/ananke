package io.palyvos.provenance.usecases.cars.cloud.noprovenance;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class CarCloudCount implements AggregateFunction<
    Tuple4<Integer, Double, Double, Long>,
    Tuple3<Integer, Integer, Long>,
                Tuple3<Integer, Integer, Long>> {

  @Override
  public Tuple3<Integer, Integer, Long> createAccumulator() {
    return Tuple3.of(0, 0, 0L);
  }

  @Override
  public Tuple3<Integer, Integer, Long> add(
      Tuple4<Integer, Double, Double, Long> t,
      Tuple3<Integer, Integer, Long> accumulator) {
    return Tuple3.of(t.f0, accumulator.f1 + 1, Math.max(t.f3, accumulator.f2));
  }

  @Override
  public Tuple3<Integer, Integer, Long> getResult(
      Tuple3<Integer, Integer, Long> accumulator) {
    return accumulator;
  }

  @Override
  public Tuple3<Integer, Integer, Long> merge(
      Tuple3<Integer, Integer, Long> acc1, Tuple3<Integer, Integer, Long> acc2) {
    throw new UnsupportedOperationException("merge");
  }
}
