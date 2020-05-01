package io.palyvos.provenance.usecases.smartgrid.noprovenance;

import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.CountAggregateFunction.CountAccumulator;
import io.palyvos.provenance.util.BaseAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CountAggregateFunction implements AggregateFunction<SmartGridTuple, CountAccumulator
    , CountTuple> {


  @Override
  public CountAccumulator createAccumulator() {
    return new CountAccumulator();
  }

  @Override
  public CountAccumulator add(SmartGridTuple value, CountAccumulator accumulator) {
    accumulator.add(value);
    return accumulator;
  }

  @Override
  public CountTuple getResult(CountAccumulator accumulator) {
    return accumulator.getAggregatedResult();
  }

  @Override
  public CountAccumulator merge(CountAccumulator a, CountAccumulator b) {
    return a.merge(b);
  }

  public static class CountAccumulator extends
      BaseAccumulator<SmartGridTuple, CountTuple, CountAccumulator> {

    private long count;
    private String key = "";
    private long timestamp = -1;
    private long stimulus;

    @Override
    public void doAdd(SmartGridTuple tuple) {
      timestamp = Math.max(timestamp, tuple.getTimestamp());
      stimulus = Math.max(stimulus, tuple.getStimulus());
      count += 1;
    }

    @Override
    public CountTuple doGetAggregatedResult() {
      return new CountTuple(timestamp, key, stimulus, count);
    }

    @Override
    protected void doMerge(CountAccumulator other) {
      throw new UnsupportedOperationException("merge");
    }
  }
}
