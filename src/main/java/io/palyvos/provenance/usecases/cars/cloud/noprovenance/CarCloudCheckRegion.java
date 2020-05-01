package io.palyvos.provenance.usecases.cars.cloud.noprovenance;

import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.LOWER_LATITUDE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.LOWER_LONGITUDE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.UPPER_LATITUDE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.UPPER_LONGITUDE;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class CarCloudCheckRegion implements
    FilterFunction<Tuple4<Integer, Double, Double, Long>> {

  @Override
  public boolean filter(Tuple4<Integer, Double, Double, Long> t) throws Exception {
    return (t.f1 < UPPER_LATITUDE)
        && (t.f1 > LOWER_LATITUDE)
        && (t.f2 < UPPER_LONGITUDE)
        && (t.f2 > LOWER_LONGITUDE);
  }
}
