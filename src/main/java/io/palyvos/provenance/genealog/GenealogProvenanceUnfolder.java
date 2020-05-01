package io.palyvos.provenance.genealog;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.AvgStat;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import io.palyvos.provenance.genealog.GenealogProvenanceUnfolder.ProvenanceTuplePair;
import java.util.Set;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class GenealogProvenanceUnfolder<T extends GenealogTuple>
    extends RichFlatMapFunction<T, ProvenanceTuplePair> {

  public static class ProvenanceTuplePair {
    public final TimestampedUIDTuple source;
    public final GenealogTuple sink;

    public ProvenanceTuplePair(TimestampedUIDTuple source, GenealogTuple sink) {
      this.source = source;
      this.sink = sink;
    }
  }

  private static final String DEFAULT_NAME = "UNFOLDER";
  protected final ExperimentSettings settings;
  private transient AvgStat latencyStatistic;
  private final String name;
  private final GenealogGraphTraverser genealogGraphTraverser;

  public GenealogProvenanceUnfolder(ExperimentSettings settings) {
    this(DEFAULT_NAME, settings);
  }

  public GenealogProvenanceUnfolder(String name, ExperimentSettings settings) {
    this.settings = settings;
    this.name = name;
    this.genealogGraphTraverser =
        new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    final int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
    this.latencyStatistic =
        new AvgStat(settings.latencyFile(taskIndex, name), settings.autoFlush());
  }

  @Override
  public void close() throws Exception {
    latencyStatistic.close();
    super.close();
  }

  @Override
  public void flatMap(T input, Collector<ProvenanceTuplePair> collector) throws Exception {
    latencyStatistic.add(System.currentTimeMillis() - input.getStimulus());
    Set<TimestampedUIDTuple> provenance = genealogGraphTraverser.getProvenance(input);
    for (TimestampedUIDTuple tuple : provenance) {
      collector.collect(new ProvenanceTuplePair(tuple, input));
    }
  }
}
