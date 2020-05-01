package io.palyvos.provenance.usecases.cars.local.noprovenance;

import io.palyvos.provenance.usecases.cars.local.LidarImageContainer.Annotation3D;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

public class CarLocalPedestriansPredicate implements
    JoinFunction<Tuple4<String, String, Annotation3D, Long>,
        Tuple4<String, String, Annotation3D, Long>, Tuple2<String, Long>> {

  @Override
  public Tuple2<String, Long> join(
      Tuple4<String, String, Annotation3D, Long> left,
      Tuple4<String, String, Annotation3D, Long> right)
      throws Exception {
    return Tuple2.of(left.f1, Math.max(left.f3, right.f3));
  }
}
