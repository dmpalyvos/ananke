package io.palyvos.provenance.usecases.cars.local.noprovenance;

import io.palyvos.provenance.usecases.cars.local.LidarImageContainer.Annotation3D;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CarLocalCount implements
    AggregateFunction<Tuple3<String, Annotation3D, Long>, Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>> {

  @Override
  public Tuple3<String, Integer, Long> createAccumulator() {
    return Tuple3.of("", 0, 0L);
  }

  @Override
  public Tuple3<String, Integer, Long> add(
      Tuple3<String, Annotation3D, Long> t,
      Tuple3<String, Integer, Long> acc) {
    return Tuple3.of(t.f0, acc.f1 + 1, Math.max(t.f2, acc.f2));
  }

  @Override
  public Tuple3<String, Integer, Long> getResult(Tuple3<String, Integer, Long> acc) {
    return acc;
  }

  @Override
  public Tuple3<String, Integer, Long> merge(Tuple3<String, Integer, Long> acc1,
      Tuple3<String, Integer, Long> acc2) {
    throw new UnsupportedOperationException("merge");
  }
}
