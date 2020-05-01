package io.palyvos.provenance.usecases.smartgrid.noprovenance;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.ParallelTextSource;

public class SmartGridSource extends ParallelTextSource<SmartGridTuple> {

  public SmartGridSource(ExperimentSettings settings) {
    super(settings);
  }

  @Override
  protected SmartGridTuple getTuple(String line, int taskIndex, long idShift) {
    SmartGridTuple tuple =  SmartGridTuple.fromReading(line);
    long newId = Long.valueOf(tuple.getKey()) + idShift;
    tuple.setKey(String.valueOf(newId));
    return tuple;
  }

}
