package io.palyvos.provenance.usecases.smartgrid.provenance;

import io.palyvos.provenance.usecases.smartgrid.provenance.MeterUsageAggregateFunction.SmartMeterAccumulator;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.AggregateFunction;

public class MeterUsageAggregateFunction
    implements AggregateFunction<SmartGridTupleGL, SmartMeterAccumulator, SmartGridTupleGL> {

  private final Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier;

  public MeterUsageAggregateFunction(
      Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier) {
    this.aggregateStrategySupplier = aggregateStrategySupplier;
  }

  @Override
  public SmartMeterAccumulator createAccumulator() {
    return new SmartMeterAccumulator(aggregateStrategySupplier);
  }

  @Override
  public SmartMeterAccumulator add(SmartGridTupleGL tuple, SmartMeterAccumulator acc) {
    acc.add(tuple);
    return acc;
  }

  @Override
  public SmartGridTupleGL getResult(SmartMeterAccumulator accumulator) {
    return accumulator.getAggregatedResult();
  }

  @Override
  public SmartMeterAccumulator merge(SmartMeterAccumulator a, SmartMeterAccumulator b) {
    throw new UnsupportedOperationException("Merge not implemented for provenance windows!");
  }

  public static class SmartMeterAccumulator
      extends GenealogAccumulator<SmartGridTupleGL, SmartGridTupleGL, SmartMeterAccumulator> {

    private String key;
    private long value;
    private long stimulus;
    private long timestamp = -1;

    public SmartMeterAccumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public long getValue() {
      return value;
    }

    public void addValue(long value) {
      this.value += value;
    }

    public long getStimulus() {
      return stimulus;
    }

    public void updateStimulus(long stimulus) {
      this.stimulus = Math.max(this.stimulus, stimulus);
    }

    public long getTimestamp() {
      return timestamp;
    }

    public void updateTimestamp(long timestamp) {
      this.timestamp = Math.max(this.timestamp, timestamp);
    }

    @Override
    public void doAdd(SmartGridTupleGL tuple) {
      setKey(tuple.getKey());
      updateStimulus(tuple.getStimulus());
      updateTimestamp(tuple.getTimestamp());
      addValue(tuple.getValue());
    }

    @Override
    public SmartGridTupleGL doGetAggregatedResult() {
      return new SmartGridTupleGL(getKey(), getTimestamp(), getStimulus(), getValue());
    }

    @Override
    protected void doMerge(SmartMeterAccumulator other) {
      throw new UnsupportedOperationException("merge");
    }
  }
}
