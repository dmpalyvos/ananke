package io.palyvos.provenance.usecases.linearroad.noprovenance;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.ParallelTextSource;

public class LinearRoadSource extends ParallelTextSource<LinearRoadInputTuple> {

  public LinearRoadSource(ExperimentSettings settings) {
    super(settings);
  }

  @Override
  protected LinearRoadInputTuple getTuple(String line, int taskIndex, long idShift) {
    LinearRoadInputTuple tuple = LinearRoadInputTuple.fromReading(line);
    long newId = tuple.getVid() + idShift;
    tuple.setVid(newId);
    tuple.setKey(String.valueOf(newId));
    tuple.setXway(taskIndex);
    return tuple;
  }

}
