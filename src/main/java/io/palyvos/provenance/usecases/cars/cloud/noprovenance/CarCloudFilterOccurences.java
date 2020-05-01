package io.palyvos.provenance.usecases.cars.cloud.noprovenance;

import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.AREA_MIN_COUNT;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CarCloudFilterOccurences implements
    FilterFunction<Tuple3<Integer, Integer, Long>> {

  @Override
  public boolean filter(Tuple3<Integer, Integer, Long> t) throws Exception {
    return t.f1 > AREA_MIN_COUNT;
  }
}
