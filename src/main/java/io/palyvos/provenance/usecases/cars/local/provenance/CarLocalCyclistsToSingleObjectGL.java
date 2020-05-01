package io.palyvos.provenance.usecases.cars.local.provenance;

import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.usecases.cars.Tuple3GL;
import io.palyvos.provenance.usecases.cars.local.LidarImageContainer.Annotation3D;
import java.util.Map;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class CarLocalCyclistsToSingleObjectGL implements
    FlatMapFunction<CarLocalInputTupleGL, Tuple3GL<String, Annotation3D, Long>> {

  @Override
  public void flatMap(CarLocalInputTupleGL t_in,
      Collector<Tuple3GL<String, Annotation3D, Long>> collector)
      throws Exception {
    Map<String, Annotation3D> lidarObjects = t_in.f1.getAnnotations();
    for (Map.Entry<String, Annotation3D> entry : lidarObjects.entrySet()) {
      Tuple3GL<String, Annotation3D, Long> result = new Tuple3GL<>(entry.getKey(), entry.getValue(),
          t_in.getStimulus());
      result.setTimestamp(t_in.getTimestamp());
      GenealogMapHelper.INSTANCE.annotateResult(t_in, result);
      collector.collect(result);
    }
  }
}
