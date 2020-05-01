package io.palyvos.provenance.usecases.cars.cloud.provenance;

import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.AVERAGE_SPEED_MINIMUM;

import io.palyvos.provenance.usecases.cars.Tuple2GL;
import org.apache.flink.api.common.functions.FilterFunction;

public class CarCloudFilterAverageSpeedGL implements FilterFunction<Tuple2GL<Double, Long>> {

  @Override
  public boolean filter(Tuple2GL<Double, Long> t) throws Exception {
    return t.f0 > AVERAGE_SPEED_MINIMUM;
  }
}
