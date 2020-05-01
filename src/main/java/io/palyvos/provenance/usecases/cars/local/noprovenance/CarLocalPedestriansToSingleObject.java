package io.palyvos.provenance.usecases.cars.local.noprovenance;

import io.palyvos.provenance.usecases.cars.local.CarLocalInputTuple;
import io.palyvos.provenance.usecases.cars.local.LidarImageContainer.Annotation3D;
import java.util.Map;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class CarLocalPedestriansToSingleObject implements
    FlatMapFunction<CarLocalInputTuple, Tuple4<String, String, Annotation3D, Long>> {

  @Override
  public void flatMap(CarLocalInputTuple t_in,
      Collector<Tuple4<String, String, Annotation3D, Long>> collector)
      throws Exception {
    // annotations as such: object_ID : object_name, x, y, z
    Map<String, Annotation3D> left_objects = t_in.f2.getAnnotations();
    String left_payload_type = t_in.f2.getPayloadType();
    for (Map.Entry<String, Annotation3D> entry : left_objects.entrySet()) {
      collector.collect(
          Tuple4.of(left_payload_type, entry.getKey(), entry.getValue(), t_in.getStimulus()));
    }
    Map<String, Annotation3D> right_objects = t_in.f3.getAnnotations();
    String right_payload_type = t_in.f3.getPayloadType();
    for (Map.Entry<String, Annotation3D> entry : right_objects.entrySet()) {
      collector.collect(
          Tuple4.of(right_payload_type, entry.getKey(), entry.getValue(), t_in.getStimulus()));
    }
  }
}
