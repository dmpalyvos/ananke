package io.palyvos.provenance.usecases.cars.cloud.provenance;

import io.palyvos.provenance.genealog.GenealogAccumulator;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.usecases.cars.Tuple3GL;
import io.palyvos.provenance.usecases.cars.Tuple4GL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudCountGL.Accumulator;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CarCloudCountGL implements AggregateFunction<
    Tuple4GL<Integer, Double, Double, Long>,
    Accumulator,
    Tuple3GL<Integer, Integer, Long>> {

  private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

  public CarCloudCountGL(
      Supplier<ProvenanceAggregateStrategy> strategySupplier) {
    this.strategySupplier = strategySupplier;
  }

  @Override
  public Accumulator createAccumulator() {
    return new Accumulator(strategySupplier);
  }

  @Override
  public Accumulator add(
      Tuple4GL<Integer, Double, Double, Long> t,
      Accumulator accumulator) {
    accumulator.add(t);
    return accumulator;
  }

  @Override
  public Tuple3GL<Integer, Integer, Long> getResult(
      Accumulator accumulator) {
    return accumulator.getAggregatedResult();
  }

  @Override
  public Accumulator merge(
      Accumulator acc1, Accumulator acc2) {
    return acc1.merge(acc2);
  }

  public static class Accumulator extends
      GenealogAccumulator<Tuple4GL<Integer, Double, Double, Long>, Tuple3GL<Integer, Integer, Long>, Accumulator> {

    private final Tuple3GL<Integer, Integer, Long> data = new Tuple3GL<>(0, 0, 0L);

    public Accumulator(
        Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    @Override
    protected void doAdd(Tuple4GL<Integer, Double, Double, Long> tuple) {
      data.f0 = tuple.f0;
      data.f1 += 1;
      data.f2 = Math.max(tuple.f3, data.f2);
      data.setTimestamp(Math.max(tuple.getTimestamp(), data.getTimestamp()));
    }

    @Override
    protected Tuple3GL<Integer, Integer, Long> doGetAggregatedResult() {
      data.setStimulus(data.f2);
      return data;
    }

    @Override
    protected void doMerge(Accumulator other) {
      throw new UnsupportedOperationException("merge");
    }
  }
}
