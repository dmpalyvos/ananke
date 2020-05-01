package io.palyvos.provenance.usecases.cars.cloud.noprovenance;

import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.AVERAGE_SPEED_MINIMUM;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CarCloudFilterAverageSpeed implements FilterFunction<Tuple2<Double, Long>> {

  @Override
  public boolean filter(Tuple2<Double, Long> t) throws Exception {
    return t.f0 > AVERAGE_SPEED_MINIMUM;
  }
}
