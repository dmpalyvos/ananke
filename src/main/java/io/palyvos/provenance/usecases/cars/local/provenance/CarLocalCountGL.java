package io.palyvos.provenance.usecases.cars.local.provenance;

import io.palyvos.provenance.genealog.GenealogAccumulator;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.usecases.cars.Tuple3GL;
import io.palyvos.provenance.usecases.cars.local.LidarImageContainer.Annotation3D;
import io.palyvos.provenance.usecases.cars.local.provenance.CarLocalCountGL.Accumulator;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CarLocalCountGL implements
    AggregateFunction<Tuple3GL<String, Annotation3D, Long>, Accumulator, Tuple3GL<String, Integer, Long>> {

  private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

  public CarLocalCountGL(
      Supplier<ProvenanceAggregateStrategy> strategySupplier) {
    this.strategySupplier = strategySupplier;
  }

  @Override
  public Accumulator createAccumulator() {
    return new Accumulator(strategySupplier);
  }

  @Override
  public Accumulator add(Tuple3GL<String, Annotation3D, Long> value,
      Accumulator accumulator) {
    accumulator.add(value);
    return accumulator;
  }

  @Override
  public Tuple3GL<String, Integer, Long> getResult(Accumulator accumulator) {
    return accumulator.getAggregatedResult();
  }

  @Override
  public Accumulator merge(Accumulator a, Accumulator b) {
    return null;
  }

  public static class Accumulator extends
      GenealogAccumulator<Tuple3GL<String, Annotation3D, Long>, Tuple3GL<String, Integer, Long>, Accumulator> {

    private final Tuple3GL<String, Integer, Long> data = new Tuple3GL<>("", 0, 0L);

    public Accumulator(
        Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    @Override
    protected void doAdd(Tuple3GL<String, Annotation3D, Long> tuple) {
      data.f0 = tuple.f0;
      data.f1 += 1;
      data.f2 = Math.max(tuple.f2, data.f2);
      data.setTimestamp(tuple.getTimestamp());
    }

    @Override
    protected Tuple3GL<String, Integer, Long> doGetAggregatedResult() {
      data.setStimulus(data.f2);
      return data;
    }

  }
}
