package io.palyvos.provenance.usecases.cars.local.provenance;

import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.CYCLIST_MIN_NUMBER;

import io.palyvos.provenance.usecases.cars.Tuple3GL;
import org.apache.flink.api.common.functions.FilterFunction;

public class CarLocalFilterCyclesInFrontGL implements
    FilterFunction<Tuple3GL<String, Integer, Long>> {

  @Override
  public boolean filter(Tuple3GL<String, Integer, Long> t) throws Exception {
    return t.f1 > CYCLIST_MIN_NUMBER;
  }
}
