package io.palyvos.provenance.genealog;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.AvgStat;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Sink that WRITES {@link GenealogTuple} stream (including provenance) to file and records
 * latency.
 */
public class GenealogFileSink<T extends GenealogTuple> extends RichSinkFunction<T> {

  private static final String DEFAULT_NAME = "SINK";
  protected final ExperimentSettings settings;
  private transient AvgStat latencyStatistic;
  private transient PrintWriter pw;
  protected final String name;
  private final GenealogGraphTraverser genealogGraphTraverser;

  public GenealogFileSink(ExperimentSettings settings) {
    this(DEFAULT_NAME, settings);
  }

  public GenealogFileSink(String name, ExperimentSettings settings) {
    this.settings = settings;
    this.name = name;
    this.genealogGraphTraverser = new GenealogGraphTraverser(
        settings.aggregateStrategySupplier().get());
  }

  public static <T extends GenealogTuple> GenealogFileSink<T> newInstance(
      ExperimentSettings settings) {
    return (settings.graphTraversalStatistics()) ?
        new GenealogFileSinkTraversalStatistics<>(settings) :
        new GenealogFileSink<>(settings);
  }

  public static <T extends GenealogTuple> GenealogFileSink<T> newInstance(
      String name, ExperimentSettings settings) {
    return (settings.graphTraversalStatistics()) ?
        new GenealogFileSinkTraversalStatistics<>(name, settings) :
        new GenealogFileSink<>(name, settings);

  }


  @Override
  public void open(Configuration parameters) throws Exception {
    final int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
    this.latencyStatistic = new AvgStat(settings.latencyFile(taskIndex, name),
        settings.autoFlush());
    try {
      pw = new PrintWriter(new FileWriter(settings.outputFile(taskIndex, name)),
          settings.autoFlush());
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
    super.open(parameters);
  }

  @Override
  public void invoke(T tuple, Context context) {
    latencyStatistic.add(System.currentTimeMillis() - tuple.getStimulus());
    Set<TimestampedUIDTuple> provenance = getProvenance(tuple);
    pw.println(formattedProvenance(tuple, provenance));
  }

  protected Set<TimestampedUIDTuple> getProvenance(T tuple) {
    return genealogGraphTraverser.getProvenance(tuple);
  }

  private String formattedProvenance(T tuple, Set<TimestampedUIDTuple> provenance) {
    StringBuffer sb = new StringBuffer();
    sb.append("> {").append(tuple).append("} <- [").append(provenance.size()).append("] {\n");
    for (TimestampedUIDTuple t : provenance) {
      sb.append(t).append("\n");
    }
    sb.append("}");
    return sb.toString();
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
