package io.palyvos.provenance.usecases.cars.cloud.provenance;

import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.LOWER_LATITUDE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.LOWER_LONGITUDE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.UPPER_LATITUDE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.UPPER_LONGITUDE;

import io.palyvos.provenance.usecases.cars.Tuple4GL;
import org.apache.flink.api.common.functions.FilterFunction;

public class CarCloudCheckRegionGL implements
    FilterFunction<Tuple4GL<Integer, Double, Double, Long>> {

  @Override
  public boolean filter(Tuple4GL<Integer, Double, Double, Long> t) throws Exception {
    return (t.f1 < UPPER_LATITUDE)
        && (t.f1 > LOWER_LATITUDE)
        && (t.f2 < UPPER_LONGITUDE)
        && (t.f2 > LOWER_LONGITUDE);
  }
}
