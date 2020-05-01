package io.palyvos.provenance.usecases.cars.cloud.provenance;

import io.palyvos.provenance.usecases.cars.Tuple3GL;
import org.apache.flink.api.common.functions.FilterFunction;

public class CarCloudFirstFilterGL implements
    FilterFunction<Tuple3GL<CarCloudInputTupleGL, CarCloudInputTupleGL, Long>> {

  @Override
  public boolean filter(Tuple3GL<CarCloudInputTupleGL, CarCloudInputTupleGL, Long> t)
      throws Exception {
    return !(t.f1.f3 == -1.0);
  }
}
