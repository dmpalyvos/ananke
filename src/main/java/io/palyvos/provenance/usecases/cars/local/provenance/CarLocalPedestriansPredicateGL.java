package io.palyvos.provenance.usecases.cars.local.provenance;

import io.palyvos.provenance.genealog.GenealogJoinHelper;
import io.palyvos.provenance.usecases.cars.Tuple2GL;
import io.palyvos.provenance.usecases.cars.Tuple4GL;
import io.palyvos.provenance.usecases.cars.local.LidarImageContainer.Annotation3D;
import org.apache.flink.api.common.functions.JoinFunction;

public class CarLocalPedestriansPredicateGL implements
    JoinFunction<Tuple4GL<String, String, Annotation3D, Long>,
        Tuple4GL<String, String, Annotation3D, Long>, Tuple2GL<String, Long>> {

  @Override
  public Tuple2GL<String, Long> join(
      Tuple4GL<String, String, Annotation3D, Long> left,
      Tuple4GL<String, String, Annotation3D, Long> right)
      throws Exception {
    Tuple2GL<String, Long> result = new Tuple2GL<>(left.f1, Math.max(left.f3, right.f3));
    result.setTimestamp(Math.max(left.getTimestamp(), right.getTimestamp()));
    result.setStimulus(result.f1);
    GenealogJoinHelper.INSTANCE.annotateResult(left, right, result);
    return result;
  }
}
