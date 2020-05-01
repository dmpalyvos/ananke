package io.palyvos.provenance.util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.function.Function;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class LatencyLoggingSink<T> extends RichSinkFunction<T> {

  private static final String DEFAULT_NAME = "SINK";
  private final ExperimentSettings settings;
  private transient AvgStat latencyStatistic;
  private transient PrintWriter pw;
  private final String name;
  private final Function<T, Long> latencyFunction;

  public static <T> LatencyLoggingSink<T> newInstance(Function<T, Long> latencyFunction, ExperimentSettings settings) {
    return LatencyLoggingSink.newInstance(DEFAULT_NAME, latencyFunction, settings);
  }
  public static <T> LatencyLoggingSink<T> newInstance(
      String name, Function<T, Long> latencyFunction, ExperimentSettings settings) {
    return new LatencyLoggingSink<>(name, latencyFunction, settings);
  }

  public static <T extends BaseTuple> LatencyLoggingSink<T> newInstance(
      ExperimentSettings settings) {
    return LatencyLoggingSink.newInstance(DEFAULT_NAME, settings);
  }

  public static <T extends BaseTuple> LatencyLoggingSink<T> newInstance(
      String name, ExperimentSettings settings) {
    return new LatencyLoggingSink<>(name, (Serializable & Function<T, Long>) BaseTuple::getStimulus, settings);
  }

  public LatencyLoggingSink(String name, Function<T, Long> latencyFunction,
      ExperimentSettings settings) {
    this.settings = settings;
    this.name = name;
    this.latencyFunction = latencyFunction;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    final int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
    this.latencyStatistic =
        new AvgStat(settings.latencyFile(taskIndex, name), settings.autoFlush());
    try {
      pw =
          new PrintWriter(new FileWriter(settings.outputFile(taskIndex, name)), settings.autoFlush());
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
    super.open(parameters);
  }

  @Override
  public void invoke(T tuple, Context context) {
    latencyStatistic.add(System.currentTimeMillis() - latencyFunction.apply(tuple));
    pw.println(tuple);
  }

  @Override
  public void close() throws Exception {
    latencyStatistic.close();
    pw.print("--- OUTPUT END ---");
    pw.flush();
    pw.close();
    super.close();
  }
}
