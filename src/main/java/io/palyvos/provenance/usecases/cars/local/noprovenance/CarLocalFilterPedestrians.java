package io.palyvos.provenance.usecases.cars.local.noprovenance;

import io.palyvos.provenance.usecases.cars.local.LidarImageContainer.Annotation3D;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class CarLocalFilterPedestrians implements
    FilterFunction<Tuple4<String, String, Annotation3D, Long>> {

  @Override
  public boolean filter(
      Tuple4<String, String, Annotation3D, Long> t)
      throws Exception {
    return t.f2.labelClass.equals("PEDESTRIAN");
  }
}
