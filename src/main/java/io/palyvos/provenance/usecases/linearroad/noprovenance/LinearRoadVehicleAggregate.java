package io.palyvos.provenance.usecases.linearroad.noprovenance;

import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadVehicleAggregate.VehicleAccumulator;
import io.palyvos.provenance.util.BaseAccumulator;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.functions.AggregateFunction;

public class LinearRoadVehicleAggregate implements
    AggregateFunction<LinearRoadInputTuple, VehicleAccumulator, VehicleTuple> {

  @Override
  public VehicleAccumulator createAccumulator() {
    return new VehicleAccumulator();
  }

  @Override
  public VehicleAccumulator add(LinearRoadInputTuple tuple,
      VehicleAccumulator acc) {
    acc.add(tuple);
    return acc;
  }

  @Override
  public VehicleTuple getResult(VehicleAccumulator acc) {
    return acc.getAggregatedResult();
  }

  @Override
  public VehicleAccumulator merge(VehicleAccumulator a,
      VehicleAccumulator b) {
    return a.merge(b);
  }

  public static class VehicleAccumulator extends BaseAccumulator<LinearRoadInputTuple,
          VehicleTuple, VehicleAccumulator> {

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

    @Override
    public void doAdd(LinearRoadInputTuple t) {
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
      String posString = t.getXway() + "," + t.getLane() + "," + t.getDir() + ","
          + t.getSeg() + "," + t.getPos();
      positions.add(posString);
    }

    @Override
    public VehicleTuple doGetAggregatedResult() {
      boolean uniquePosition = positions.size() == 1;
      VehicleTuple result = new VehicleTuple(timestamp, Integer.valueOf(key),
          counter, latestXWay, latestLane, latestDir, latestSeg,
          latestPos, uniquePosition, stimulus);
      return result;
    }

    @Override
    protected void doMerge(VehicleAccumulator other) {
      throw new UnsupportedOperationException("merge");
    }
  }

}
