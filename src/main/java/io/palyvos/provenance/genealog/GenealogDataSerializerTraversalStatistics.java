package io.palyvos.provenance.genealog;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.AvgStat;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import java.util.Set;

public class GenealogDataSerializerTraversalStatistics extends GenealogDataSerializer {

  private static final String TASK_NAME = "SerializerTraverser";
  private String statisticsFolder;
  private transient AvgStat traversalStatistic;

  public GenealogDataSerializerTraversalStatistics(
      ProvenanceAggregateStrategy provenanceAggregateStrategy,
      String statisticsFolder) {
    super(provenanceAggregateStrategy);
    this.statisticsFolder = statisticsFolder;
  }

  public GenealogDataSerializerTraversalStatistics() {
    // For serialization/deserialization purposes
  }

  @Override
  public void write(Kryo kryo, Output output, GenealogData object) {
    initStatistic();
    super.write(kryo, output, object);
  }

  @Override
  protected Set<TimestampedUIDTuple> getProvenance(GenealogData object) {
    long start = System.nanoTime();
    Set<TimestampedUIDTuple> provenance = super.getProvenance(object);
    traversalStatistic.add(System.nanoTime() - start);
    traversalStatistic.flush();
    return provenance;
  }

  private void initStatistic() {
    if (traversalStatistic == null) {
      traversalStatistic =
          new AvgStat(ExperimentSettings.uniqueStatisticsFile(TASK_NAME, statisticsFolder,
              ExperimentSettings.TRAVERSAL_FILE, "csv"), true);
    }
  }

}
