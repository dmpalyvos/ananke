package io.palyvos.provenance.usecases.smartgrid.provenance;

import io.palyvos.provenance.usecases.CountTupleGL;
import io.palyvos.provenance.usecases.smartgrid.provenance.CountAggregateFunction.CountAccumulator;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CountAggregateFunction implements AggregateFunction<SmartGridTupleGL, CountAccumulator
    , CountTupleGL> {

  private final Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier;

  public CountAggregateFunction(
      Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier) {
    this.aggregateStrategySupplier = aggregateStrategySupplier;
  }

  @Override
  public CountAccumulator createAccumulator() {
    return new CountAccumulator(aggregateStrategySupplier);
  }

  @Override
  public CountAccumulator add(SmartGridTupleGL value, CountAccumulator accumulator) {
    accumulator.add(value);
    return accumulator;
  }

  @Override
  public CountTupleGL getResult(CountAccumulator accumulator) {
    return accumulator.getAggregatedResult();
  }

  @Override
  public CountAccumulator merge(CountAccumulator a, CountAccumulator b) {
    throw new UnsupportedOperationException("Merge not implemented for provenance windows!");
  }

  public static class CountAccumulator extends
      GenealogAccumulator<SmartGridTupleGL, CountTupleGL, CountAccumulator> {

    private long count;
    private String key = "";
    private long timestamp = -1;
    private long stimulus;

    public CountAccumulator(
        Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    @Override
    public void doAdd(SmartGridTupleGL tuple) {
      timestamp = Math.max(timestamp, tuple.getTimestamp());
      stimulus = Math.max(stimulus, tuple.getStimulus());
      count += 1;
    }

    @Override
    public CountTupleGL doGetAggregatedResult() {
      return new CountTupleGL(timestamp, key, stimulus, count);
    }

    @Override
    protected void doMerge(CountAccumulator other) {
      throw new UnsupportedOperationException("merge");
    }
  }
}
