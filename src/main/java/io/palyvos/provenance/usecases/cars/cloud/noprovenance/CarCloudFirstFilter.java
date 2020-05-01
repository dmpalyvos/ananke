package io.palyvos.provenance.usecases.cars.cloud.noprovenance;

import io.palyvos.provenance.usecases.cars.cloud.CarCloudInputTuple;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CarCloudFirstFilter implements
    FilterFunction<Tuple3<CarCloudInputTuple, CarCloudInputTuple, Long>> {

  @Override
  public boolean filter(Tuple3<CarCloudInputTuple, CarCloudInputTuple, Long> t)
      throws Exception {
    return !(t.f1.f3 == -1.0);
  }
}
