package io.palyvos.provenance.util;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class LatencyLoggingMap<T extends TimestampedTuple, OUT> extends RichMapFunction<T, OUT> {
  private final ExperimentSettings settings;
  private transient AvgStat latencyStatistic;
  private final String name;

  public LatencyLoggingMap(String name, ExperimentSettings settings) {
    this.settings = settings;
    this.name = name;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    final int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
    this.latencyStatistic =
        new AvgStat(settings.latencyFile(taskIndex, name), settings.autoFlush());
    super.open(parameters);
  }

  @Override
  public OUT map(T tuple) {
    latencyStatistic.add(System.currentTimeMillis() - tuple.getStimulus());
    return (OUT) tuple;
  }

  @Override
  public void close() throws Exception {
    latencyStatistic.close();
    super.close();
  }

}
