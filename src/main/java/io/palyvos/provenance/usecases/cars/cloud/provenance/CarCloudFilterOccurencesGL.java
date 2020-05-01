package io.palyvos.provenance.usecases.cars.cloud.provenance;

import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.AREA_MIN_COUNT;

import io.palyvos.provenance.usecases.cars.Tuple3GL;
import org.apache.flink.api.common.functions.FilterFunction;

public class CarCloudFilterOccurencesGL implements
    FilterFunction<Tuple3GL<Integer, Integer, Long>> {

  @Override
  public boolean filter(Tuple3GL<Integer, Integer, Long> t) throws Exception {
    return t.f1 > AREA_MIN_COUNT;
  }
}
