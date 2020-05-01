package io.palyvos.provenance.usecases.smartgrid.provenance;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.ParallelTextSource;
import org.apache.flink.configuration.Configuration;

public class SmartGridSourceGL extends ParallelTextSource<SmartGridTupleGL> {

  public SmartGridSourceGL(ExperimentSettings settings) {
    super(settings);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  protected SmartGridTupleGL getTuple(String line, int taskIndex, long idShift) {
    SmartGridTupleGL tuple = SmartGridTupleGL.fromReading(line);
    long newId = Long.valueOf(tuple.getKey()) + idShift;
    tuple.setKey(String.valueOf(newId));
    return tuple;
  }
}
