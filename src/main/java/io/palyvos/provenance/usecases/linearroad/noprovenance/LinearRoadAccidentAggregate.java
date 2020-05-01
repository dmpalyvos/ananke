package io.palyvos.provenance.usecases.linearroad.noprovenance;

import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadAccidentAggregate.AccidentCountAccumulator;
import io.palyvos.provenance.util.BaseAccumulator;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.functions.AggregateFunction;

public class LinearRoadAccidentAggregate implements AggregateFunction<VehicleTuple,
    AccidentCountAccumulator, CountTuple> {

  @Override
  public AccidentCountAccumulator createAccumulator() {
    return new AccidentCountAccumulator();
  }

  @Override
  public AccidentCountAccumulator add(VehicleTuple value, AccidentCountAccumulator accumulator) {
    accumulator.add(value);
    return accumulator;
  }

  @Override
  public CountTuple getResult(AccidentCountAccumulator accumulator) {
    return accumulator.getAggregatedResult();
  }

  @Override
  public AccidentCountAccumulator merge(AccidentCountAccumulator a, AccidentCountAccumulator b) {
    return a.merge(b);
  }

  public static class AccidentCountAccumulator extends BaseAccumulator<VehicleTuple,
      CountTuple, AccidentCountAccumulator> {

    private final transient Set<Integer> carIds = new HashSet<>();
    private long timestamp = -1;
    private long stimulus;
    private String key;

    @Override
    public void doAdd(VehicleTuple tuple) {
      timestamp = Math.max(timestamp, tuple.getTimestamp());
      stimulus = Math.max(stimulus, tuple.getStimulus());
      key = tuple.getKey();
      carIds.add(tuple.getVid());
    }

    @Override
    public CountTuple doGetAggregatedResult() {
      return new CountTuple(timestamp, key, stimulus, carIds.size());
    }

    @Override
    protected void doMerge(AccidentCountAccumulator other) {
      throw new UnsupportedOperationException("merge");
    }
  }

}
