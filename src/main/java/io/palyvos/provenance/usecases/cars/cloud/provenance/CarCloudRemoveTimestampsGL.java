package io.palyvos.provenance.usecases.cars.cloud.provenance;

import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.usecases.cars.Tuple4GL;
import org.apache.flink.api.common.functions.MapFunction;

public class CarCloudRemoveTimestampsGL implements
    MapFunction<CarCloudInputTupleGL, Tuple4GL<Integer, Double, Double , Long>> {

  @Override
  public Tuple4GL<Integer, Double, Double, Long> map(CarCloudInputTupleGL t)
      throws Exception {
    Tuple4GL<Integer, Double, Double, Long> result = new Tuple4GL<>(t.f1,
        t.f2, t.f3, t.getStimulus());
    result.setTimestamp(t.getTimestamp());
    GenealogMapHelper.INSTANCE.annotateResult(t, result);
    return result;
  }
}
