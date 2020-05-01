package io.palyvos.provenance.genealog;

import io.palyvos.provenance.util.AvgStat;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.util.Set;
import org.apache.flink.configuration.Configuration;

public class GenealogLatencyLoggingSinkTraversalStatistics<T extends GenealogTuple> extends
    GenealogLatencyLoggingSink<T> {

  private transient AvgStat traversalStatistic;

  public GenealogLatencyLoggingSinkTraversalStatistics(
      ExperimentSettings settings) {
    super(settings);
  }

  public GenealogLatencyLoggingSinkTraversalStatistics(String name,
      ExperimentSettings settings) {
    super(name, settings);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    String sinkId = String.format("sink%d", getRuntimeContext().getIndexOfThisSubtask());
    traversalStatistic =
        new AvgStat(ExperimentSettings
            .hostnameStatisticsFile(name, sinkId, settings.statisticsFolder(),
                ExperimentSettings.TRAVERSAL_FILE, "csv"), true);
  }

  @Override
  protected Set<TimestampedUIDTuple> getProvenance(T tuple) {
    long start = System.nanoTime();
    Set<TimestampedUIDTuple> provenance = super.getProvenance(tuple);
    traversalStatistic.add(System.nanoTime() - start);
    return provenance;
  }

  @Override
  public void close() throws Exception {
    traversalStatistic.close();
    super.close();
  }
}
