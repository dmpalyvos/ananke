package io.palyvos.provenance.usecases.cars.local.noprovenance;

import io.palyvos.provenance.usecases.cars.local.CarLocalInputTuple;
import io.palyvos.provenance.usecases.cars.local.LidarImageContainer.Annotation3D;
import java.util.Map;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class CarLocalCyclistsToSingleObject implements
    FlatMapFunction<CarLocalInputTuple, Tuple3<String, Annotation3D, Long>> {

  @Override
  public void flatMap(CarLocalInputTuple t_in,
      Collector<Tuple3<String, Annotation3D, Long>> collector)
      throws Exception {
    Map<String, Annotation3D> lidarObjects = t_in.f1.getAnnotations();
    for (Map.Entry<String, Annotation3D> entry : lidarObjects.entrySet()) {
      collector.collect(Tuple3.of(entry.getKey(), entry.getValue(), t_in.getStimulus()));
    }
  }
}
