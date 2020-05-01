package io.palyvos.provenance.usecases.cars.local.noprovenance;

import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.CYCLIST_Y_AREA;
import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.CYCLIST_Z_AREA;

import io.palyvos.provenance.usecases.cars.local.LidarImageContainer.Annotation3D;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CarLocalFilterBicycles implements
    FilterFunction<Tuple3<String, Annotation3D, Long>> {

  @Override
  public boolean filter(
      Tuple3<String, Annotation3D, Long> t)
      throws Exception {
    return (t.f1.labelClass.equals("BICYCLE")) && (-CYCLIST_Y_AREA < t.f1.x) && (t.f1.x
        < CYCLIST_Y_AREA)
        && (-CYCLIST_Z_AREA < t.f1.y) && (t.f1.y < CYCLIST_Z_AREA);
  }
}
