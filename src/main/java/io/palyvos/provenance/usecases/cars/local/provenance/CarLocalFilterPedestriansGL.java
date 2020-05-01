package io.palyvos.provenance.usecases.cars.local.provenance;

import io.palyvos.provenance.usecases.cars.Tuple4GL;
import io.palyvos.provenance.usecases.cars.local.LidarImageContainer.Annotation3D;
import org.apache.flink.api.common.functions.FilterFunction;

public class CarLocalFilterPedestriansGL implements
    FilterFunction<Tuple4GL<String, String, Annotation3D, Long>> {

  @Override
  public boolean filter(
      Tuple4GL<String, String, Annotation3D, Long> t)
      throws Exception {
    return t.f2.labelClass.equals("PEDESTRIAN");
  }
}
