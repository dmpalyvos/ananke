package io.palyvos.provenance.usecases.cars.cloud.provenance;

import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.LAT_CONVERSION;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.LON_CONVERSION;

import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.usecases.cars.Tuple3GL;
import org.apache.flink.api.common.functions.MapFunction;

public class CarCloudCurrentSpeedGL implements
    MapFunction<Tuple3GL<CarCloudInputTupleGL, CarCloudInputTupleGL, Long>, Tuple3GL<Integer, Double, Long>> {

  @Override
  public Tuple3GL<Integer, Double, Long> map(
      Tuple3GL<CarCloudInputTupleGL, CarCloudInputTupleGL, Long> t) throws Exception {
    double speed =
        Math.sqrt(
            Math.pow((t.f0.f2 - t.f1.f2) * LAT_CONVERSION, 2)
                + Math.pow((t.f0.f3 - t.f1.f3) * LON_CONVERSION, 2))
            / (t.f1.f0 - t.f0.f0)
            * 1000 // millisecond to second conversion
            * 3.6; // m/s to km/h conversion;

    Tuple3GL<Integer, Double, Long> result = new Tuple3GL<>(t.f0.f1, speed, t.f2);
    result.setTimestamp(t.f0.getTimestamp());
    GenealogMapHelper.INSTANCE.annotateResult(t, result);
    return result;
  }
}
