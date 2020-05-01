package io.palyvos.provenance.usecases.cars.cloud.noprovenance;

import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.LAT_CONVERSION;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.LON_CONVERSION;

import io.palyvos.provenance.usecases.cars.cloud.CarCloudInputTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CarCloudCurrentSpeed implements
    MapFunction<Tuple3<CarCloudInputTuple, CarCloudInputTuple, Long>, Tuple3<Integer, Double, Long>> {

  @Override
  public Tuple3<Integer, Double, Long> map(
      Tuple3<CarCloudInputTuple, CarCloudInputTuple, Long> t) throws Exception {
    double speed =
        Math.sqrt(
            Math.pow((t.f0.f2 - t.f1.f2) * LAT_CONVERSION, 2)
                + Math.pow((t.f0.f3 - t.f1.f3) * LON_CONVERSION, 2))
            / (t.f1.f0 - t.f0.f0)
            * 1000 // millisecond to second conversion
            * 3.6; // m/s to km/h conversion;

    return Tuple3.of(t.f0.f1, speed, t.f2);
  }
}
