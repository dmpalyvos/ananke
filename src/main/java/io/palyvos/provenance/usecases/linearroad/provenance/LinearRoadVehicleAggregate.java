package io.palyvos.provenance.usecases.linearroad.provenance;

import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadVehicleAggregate.VehicleAccumulator;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.AggregateFunction;

public class LinearRoadVehicleAggregate
    implements AggregateFunction<LinearRoadInputTupleGL, VehicleAccumulator, VehicleTupleGL> {

  private final Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier;

  public LinearRoadVehicleAggregate(Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier) {
    this.aggregateStrategySupplier = aggregateStrategySupplier;
  }

  @Override
  public VehicleAccumulator createAccumulator() {
    return new VehicleAccumulator(aggregateStrategySupplier);
  }

  @Override
  public VehicleAccumulator add(LinearRoadInputTupleGL tuple, VehicleAccumulator acc) {
    acc.add(tuple);
    return acc;
  }

  @Override
  public VehicleTupleGL getResult(VehicleAccumulator acc) {
    return acc.getAggregatedResult();
  }

  @Override
  public VehicleAccumulator merge(VehicleAccumulator a, VehicleAccumulator b) {
    throw new UnsupportedOperationException("Merge not implemented for provenance windows!");
  }

  public static class VehicleAccumulator
      extends GenealogAccumulator<LinearRoadInputTupleGL, VehicleTupleGL, VehicleAccumulator> {

    private final Set<String> positions = new HashSet<>();
    private int latestXWay = -1;
    private int latestLane = -1;
    private int latestDir = -1;
    private int latestSeg = -1;
    private int latestPos = -1;
    private long stimulus = -1;
    private int counter;
    private String key;
    private long timestamp = -1;
    private long latestTimestamp = -1;

    public VehicleAccumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    @Override
    public void doAdd(LinearRoadInputTupleGL t) {
      counter++;
      if (latestTimestamp < t.getTimestamp()) {
        latestTimestamp = t.getTimestamp();
        latestXWay = t.getXway();
        latestLane = t.getLane();
        latestDir = t.getDir();
        latestSeg = t.getSeg();
        latestPos = t.getPos();
        key = t.getKey();
      }
      stimulus = Math.max(stimulus, t.getStimulus());
      timestamp = Math.max(timestamp, t.getTimestamp());
      String posString =
          t.getXway() + "," + t.getLane() + "," + t.getDir() + "," + t.getSeg() + "," + t.getPos();
      positions.add(posString);
    }

    @Override
    public VehicleTupleGL doGetAggregatedResult() {
      boolean uniquePosition = positions.size() == 1;
      VehicleTupleGL result =
          new VehicleTupleGL(
              timestamp,
              Integer.valueOf(key),
              counter,
              latestXWay,
              latestLane,
              latestDir,
              latestSeg,
              latestPos,
              uniquePosition,
              stimulus);
      return result;
    }

    @Override
    protected void doMerge(VehicleAccumulator other) {
      counter += other.counter;
      if (latestTimestamp < other.latestTimestamp) {
        latestTimestamp = other.latestTimestamp;
        latestXWay = other.latestXWay;
        latestLane = other.latestLane;
        latestDir = other.latestDir;
        latestSeg = other.latestSeg;
        latestPos = other.latestPos;
      }
      stimulus = Math.max(stimulus, other.stimulus);
      timestamp = Math.min(timestamp, other.timestamp);
      positions.addAll(other.positions);
    }
  }
}
