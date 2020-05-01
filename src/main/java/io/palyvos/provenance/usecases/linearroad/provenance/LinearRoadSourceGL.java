package io.palyvos.provenance.usecases.linearroad.provenance;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.ParallelTextSource;
import org.apache.flink.configuration.Configuration;

public class LinearRoadSourceGL extends ParallelTextSource<LinearRoadInputTupleGL> {

  public LinearRoadSourceGL(ExperimentSettings settings) {
    super(settings);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  protected LinearRoadInputTupleGL getTuple(String line, int taskIndex, long idShift) {
    LinearRoadInputTupleGL tuple = LinearRoadInputTupleGL.fromReading(line);
    long newId = tuple.getVid() + idShift;
    tuple.setVid(newId);
    tuple.setKey(String.valueOf(newId));
    tuple.setXway(taskIndex);
    return tuple;
  }
}
