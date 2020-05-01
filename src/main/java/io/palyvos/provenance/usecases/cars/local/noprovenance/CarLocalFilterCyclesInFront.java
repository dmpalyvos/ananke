package io.palyvos.provenance.usecases.cars.local.noprovenance;

import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.CYCLIST_MIN_NUMBER;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CarLocalFilterCyclesInFront implements
    FilterFunction<Tuple3<String, Integer, Long>> {

  @Override
  public boolean filter(Tuple3<String, Integer, Long> t) throws Exception {
    return t.f1 > CYCLIST_MIN_NUMBER;
  }
}
