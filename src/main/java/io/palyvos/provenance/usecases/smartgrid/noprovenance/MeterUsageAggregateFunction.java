package io.palyvos.provenance.usecases.smartgrid.noprovenance;

import io.palyvos.provenance.usecases.smartgrid.noprovenance.MeterUsageAggregateFunction.SmartMeterAccumulator;
import io.palyvos.provenance.util.BaseAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

public class MeterUsageAggregateFunction implements
    AggregateFunction<SmartGridTuple, SmartMeterAccumulator, SmartGridTuple> {

  @Override
  public SmartMeterAccumulator createAccumulator() {
    return new SmartMeterAccumulator();
  }

  @Override
  public SmartMeterAccumulator add(SmartGridTuple tuple,
      SmartMeterAccumulator acc) {
    acc.add(tuple);
    return acc;
  }

  @Override
  public SmartGridTuple getResult(SmartMeterAccumulator accumulator) {
    return accumulator.getAggregatedResult();
  }

  @Override
  public SmartMeterAccumulator merge(SmartMeterAccumulator a,
      SmartMeterAccumulator b) {
    return a.merge(b);
  }

  public static class SmartMeterAccumulator extends
      BaseAccumulator<SmartGridTuple, SmartGridTuple, SmartMeterAccumulator> {

    private String key;
    private long value;
    private long stimulus;
    private long timestamp = -1;

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
    public void doAdd(SmartGridTuple tuple) {
      setKey(tuple.getKey());
      updateStimulus(tuple.getStimulus());
      updateTimestamp(tuple.getTimestamp());
      addValue(tuple.getValue());
    }

    @Override
    public SmartGridTuple doGetAggregatedResult() {
      return new SmartGridTuple(getKey(), getTimestamp(), getStimulus(), getValue());
    }

    @Override
    protected void doMerge(SmartMeterAccumulator other) {
      throw new UnsupportedOperationException("merge");
    }
  }

}
