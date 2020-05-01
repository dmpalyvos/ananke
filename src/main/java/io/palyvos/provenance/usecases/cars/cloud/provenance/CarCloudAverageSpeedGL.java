package io.palyvos.provenance.usecases.cars.cloud.provenance;

import io.palyvos.provenance.genealog.GenealogAccumulator;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.usecases.cars.Tuple2GL;
import io.palyvos.provenance.usecases.cars.Tuple3GL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudAverageSpeedGL.Accumulator;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class CarCloudAverageSpeedGL implements
    AggregateFunction<Tuple3GL<Integer, Double, Long>, Accumulator, Tuple2GL<Double, Long>> {

  private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

  public CarCloudAverageSpeedGL(
      Supplier<ProvenanceAggregateStrategy> strategySupplier) {
    this.strategySupplier = strategySupplier;
  }

  @Override
  public Accumulator createAccumulator() {
    return new Accumulator(strategySupplier);
  }

  @Override
  public Accumulator add(Tuple3GL<Integer, Double, Long> value, Accumulator accumulator) {
    accumulator.add(value);
    return accumulator;
  }

  @Override
  public Tuple2GL<Double, Long> getResult(Accumulator accumulator) {
    return accumulator.getAggregatedResult();
  }

  @Override
  public Accumulator merge(Accumulator a, Accumulator b) {
    return a.merge(b);
  }


  public static class Accumulator extends
      GenealogAccumulator<Tuple3GL<Integer, Double, Long>, Tuple2GL<Double, Long>, Accumulator> {

    private final Tuple4<Double, Integer, Long, Long> data = new Tuple4<>(0.0, 0, 0L,0L);

    public Accumulator(
        Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    @Override
    protected void doAdd(Tuple3GL<Integer, Double, Long> tuple) {
      // f0 -> sum
      // f1 -> count
      data.f0 += tuple.f1;
      data.f1 += 1;
      // pass on the stimulus
      data.f2 = Math.max(tuple.f2, data.f2);
      // set timestamp of data to maximum of tuples that have been added so far
      data.f3 = Math.max(tuple.getTimestamp(), data.f3);
    }

    @Override
    protected Tuple2GL<Double, Long> doGetAggregatedResult() {
      Tuple2GL<Double, Long> result = new Tuple2GL<>(data.f0 / data.f1, data.f2);
      result.setStimulus(result.f1);
      result.setTimestamp(data.f3);
      return result;
    }

    @Override
    protected void doMerge(Accumulator other) {
      throw new UnsupportedOperationException("merge");
    }


  }
}
