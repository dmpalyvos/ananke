package io.palyvos.provenance.usecases.smartgrid;

import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_AGGREGATE_MULTIPLEX_OUTPUT;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_BOTH_MULTIPLEX_OUTPUTS;

import io.palyvos.provenance.usecases.smartgrid.noprovenance.SmartGridTuple;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

@Deprecated
public class AnomalyOutputSelector<T extends SmartGridTuple> implements OutputSelector<T> {

  @Override
  public Iterable<String> select(T t) {
    if ((t.getTimestamp() + 1) % 24 == 0) {
      return ANOMALY_BOTH_MULTIPLEX_OUTPUTS;
    } else {
      return ANOMALY_AGGREGATE_MULTIPLEX_OUTPUT;
    }
  }
}
