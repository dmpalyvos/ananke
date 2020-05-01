package io.palyvos.provenance.usecases.cars.cloud.provenance;

import io.palyvos.provenance.genealog.GenealogAccumulator;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.usecases.cars.Tuple3GL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudGetWindowFirstLastGL.Accumulator;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CarCloudGetWindowFirstLastGL implements AggregateFunction<
    CarCloudInputTupleGL,
    Accumulator,
    Tuple3GL<CarCloudInputTupleGL, CarCloudInputTupleGL, Long>> {

  private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

  public CarCloudGetWindowFirstLastGL(
      Supplier<ProvenanceAggregateStrategy> strategySupplier) {
    this.strategySupplier = strategySupplier;
  }

  @Override
  public Accumulator createAccumulator() {
    return new Accumulator(strategySupplier);
  }

  @Override
  public Accumulator add(CarCloudInputTupleGL value, Accumulator accumulator) {
    accumulator.add(value);
    return accumulator;
  }

  @Override
  public Tuple3GL<CarCloudInputTupleGL, CarCloudInputTupleGL, Long> getResult(Accumulator accumulator) {
    return accumulator.getAggregatedResult();
  }

  @Override
  public Accumulator merge(Accumulator a, Accumulator b) {
    return a.merge(b);
  }


  public static class Accumulator extends
      GenealogAccumulator<CarCloudInputTupleGL, Tuple3GL<CarCloudInputTupleGL, CarCloudInputTupleGL, Long>, Accumulator> {

    private final Tuple2<CarCloudInputTupleGL, CarCloudInputTupleGL> accumulator = new Tuple2<>(
        CarCloudInputTupleGL.newEmptyInstance(),
        CarCloudInputTupleGL.newEmptyInstance());

    public Accumulator(
        Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    @Override
    protected void doAdd(CarCloudInputTupleGL value) {
      if (accumulator.f0.f3 == -1.0) {
        accumulator.f0 = value;
      } else {
        accumulator.f1 = value;
      }
    }

    @Override
    protected Tuple3GL<CarCloudInputTupleGL, CarCloudInputTupleGL, Long> doGetAggregatedResult() {
      Tuple3GL<CarCloudInputTupleGL, CarCloudInputTupleGL, Long> result =
              new Tuple3GL<>(accumulator.f0, accumulator.f1,
                      Math.max(accumulator.f0.getStimulus(), accumulator.f1.getStimulus()));
      result.setTimestamp(Math.max(accumulator.f0.getTimestamp(), accumulator.f1.getTimestamp()));
      return result;
    }

    @Override
    protected void doMerge(Accumulator other) {
      throw new UnsupportedOperationException("merge");
    }
  }
}
