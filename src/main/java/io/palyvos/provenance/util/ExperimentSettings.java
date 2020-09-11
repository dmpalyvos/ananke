package io.palyvos.provenance.util;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.ananke.aggregate.ListAggregateStrategy;
import io.palyvos.provenance.ananke.aggregate.SortedPointersAggregateStrategy;
import io.palyvos.provenance.ananke.aggregate.UnsortedPointersAggregateStrategy;
import io.palyvos.provenance.ananke.output.FileProvenanceGraphEncoder;
import io.palyvos.provenance.ananke.output.GephiProvenanceGraphEncoder;
import io.palyvos.provenance.ananke.output.NoOpProvenanceGraphEncoder;
import io.palyvos.provenance.ananke.output.ProvenanceGraphEncoder;
import io.palyvos.provenance.ananke.output.TimestampedFileProvenanceGraphEncoder;
import java.io.File;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.function.Supplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class ExperimentSettings implements Serializable {

  public static final String LATENCY_FILE = "latency";
  public static final String THROUGHPUT_FILE = "rate";
  public static final String TRAVERSAL_FILE = "traversal";
  public static final String INPUT_EXTENSION = "txt";
  public static final String DEFAULT_SLOT_SHARING_GROUP = "default";
  public static final String SECOND_SLOT_SHARING_GROUP = "group2";
  private static final String THIRD_SLOT_SHARING_GROUP = "group3";
  private static final String PROVENANCE_READ_TIME = "provreadtime";
  private static final String PROVENANCE_WRITE_TIME = "provwritetime";
  private static final String PROVENANCE_READS = "provreads";
  private static final String PROVENANCE_WRITES = "provwrites";
  private static final String DELIVERY_LATENCY = "deliverylatency";

  @Parameter(names = "--statisticsFolder", required = true, description = "path where output files will be stored")
  private String statisticsFolder;

  @Parameter(names = "--inputFile", description = "the input file of the streaming query")
  private String inputFile;

  @Parameter(names = "--outputFile", required = true, description = "the name of the file to store where the output of the query will be stored")
  private String outputFile;

  @Parameter(names = "--sourcesNumber", required = true, description = "number of sources of the streaming query")
  private int sourcesNumber = 1;

  @Parameter(names = "--autoFlush")
  private boolean autoFlush = false;

  @Parameter(names = "--sinkParallelism")
  private int sinkParallelism = 1;

  @Parameter(names = "--distributed", description = "configure the query for distributed execution")
  private boolean distributed;

  @Parameter(names = "--traversalStatistics", description = "record GeneaLog graph traversal statistics")
  private boolean traversalStatistics;

  @Parameter(names = "--sourceRepetitions", description = "number of times to repeat the source input")
  private int sourceRepetitions = 1;

  @Parameter(names = "--idShift")
  private long idShift = 0;

  @Parameter(names = "--sourceIP", description = "IP address of the remote data source")
  private String sourceIP;

  @Parameter(names = "--sourcePort", description = "port of the remote data source")
  private int sourcePort;

  @Parameter(names = "--maxParallelism", description = "maximum allowed parallelism")
  private int maxParallelism = 4;

  @Parameter(names = "--provenanceActivator", description = "provenance algorithm, e.g., ANANKE, GENEALOG, etc.")
  private ProvenanceActivator provenanceActivator = ProvenanceActivator.GENEALOG;

  @Parameter(names = "--aggregateStrategy", converter = AggregateStrategyConverter.class, description = "strategy for handling out-of-order aggregate tuples")
  private Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier =
      (Supplier<ProvenanceAggregateStrategy> & Serializable) UnsortedPointersAggregateStrategy::new;

  @Parameter(names = "--graphEncoder", description = "output encoder for the forward-provenance graph")
  private String graphEncoder = TimestampedFileProvenanceGraphEncoder.class.getSimpleName();

  @Parameter(names = "--watermarkInterval")
  private long watermarkInterval = 200;

  @Parameter(names = "--syntheticInputLength")
  private int syntheticInputLength = 1000;

  @Parameter(names = "--syntheticDelay")
  private int syntheticDelay = 10;

  @Parameter(names = "--syntheticProvenanceSize")
  private int syntheticProvenanceSize = 100;

  @Parameter(names = "--syntheticTupleSize")
  private int syntheticTupleSize = 32;

  @Parameter(names = "--syntheticSourceParallelism")
  private int syntheticSourceParallelism = 1;

  @Parameter(names = "--syntheticProvenanceOverlap")
  private int syntheticProvenanceOverlap;

  @Parameter(names = "--disableSinkChaining")
  private boolean disableSinkChaining;

  @Parameter(names = "--pollFrequencyMillis", description = "poll frequency for external DB experiments")
  private long pollFrequencyMillis = 1000;

  @Parameter(names = "--uniqueDbKeys", description = "enforce unique key contraints on relational DB experiments")
  private boolean uniqueDbKeys;

  @Parameter(names = "--dbFlowControl", description = "enforce basic flow control in external DB writer")
  private boolean dbFlowControl;

  public static ExperimentSettings newInstance(String[] args) {
    ExperimentSettings settings = new ExperimentSettings();
    JCommander.newBuilder().addObject(settings).build().parse(args);
    return settings;
  }

  public static String statisticsFile(
      String operator,
      Object taskIndex,
      String statisticsFolder,
      String filename,
      String fileExtension) {
    return new StringBuilder(statisticsFolder)
        .append(File.separator)
        .append(filename)
        .append("_")
        .append(operator)
        .append("_")
        .append(taskIndex)
        .append(".")
        .append(fileExtension)
        .toString();
  }

  public static String hostnameStatisticsFile(
      String operator,
      Object taskId,
      String statisticsFolder,
      String filename,
      String fileExtension) {
    String host = ManagementFactory.getRuntimeMXBean().getName();
    return statisticsFile(
        operator, String.format("%s_%s", host, taskId), statisticsFolder, filename, fileExtension);
  }

  public static String uniqueStatisticsFile(
      String operator, String statisticsFolder, String filename, String fileExtension) {
    String taskId = RandomStringUtils.randomAlphanumeric(10);
    return hostnameStatisticsFile(operator, taskId, statisticsFolder, filename, fileExtension);
  }

  public String secondSlotSharingGroup() {
    // If distributeHeavyOperators == false, assign all ops
    // to Flink's "default" co-location group (i.e., don't distribute to different slots)
    return distributed ? SECOND_SLOT_SHARING_GROUP : DEFAULT_SLOT_SHARING_GROUP;
  }

  public String thirdSlotSharingGroup() {
    // If distributeHeavyOperators == false, assign all ops
    // to Flink's "default" co-location group (i.e., don't distribute to different slots)
    return distributed ? THIRD_SLOT_SHARING_GROUP : DEFAULT_SLOT_SHARING_GROUP;
  }

  public boolean autoFlush() {
    return autoFlush;
  }

  public Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier() {
    return aggregateStrategySupplier;
  }

  public boolean dbFlowControl() {
    return dbFlowControl;
  }

  public String inputFile() {
    return String.format("%s.%s", inputFile, INPUT_EXTENSION);
  }

  public String statisticsFolder() {
    return statisticsFolder;
  }

  public int sinkParallelism() {
    return sinkParallelism;
  }

  public String latencyFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), LATENCY_FILE, "csv");
  }

  public String throughputFile(String operator, int taskIndex) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), THROUGHPUT_FILE, "csv");
  }

  public String outputFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), outputFile, "out");
  }

  public String provenanceReadsFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), PROVENANCE_READS, "csv");
  }

  public String provenanceWritesFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), PROVENANCE_WRITES, "csv");
  }

  public String provenanceReadTimeFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), PROVENANCE_READ_TIME, "csv");
  }

  public String provenanceWriteTimeFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), PROVENANCE_WRITE_TIME, "csv");
  }

  public String deliveryLatencyFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), DELIVERY_LATENCY, "csv");
  }

  public long idShift() {
    return idShift;
  }

  public String sourceIP() {
    return sourceIP;
  }

  public int sourcePort() {
    return sourcePort;
  }

  public int sourcesNumber() {
    return sourcesNumber;
  }

  public int maxParallelism() {
    return maxParallelism;
  }

  public int sourceRepetitions() {
    return sourceRepetitions;
  }

  public long getWatermarkInterval() {
    return watermarkInterval;
  }

  public int syntheticInputSize() {
    return syntheticInputLength;
  }

  public int syntheticDelay() {
    return syntheticDelay;
  }

  public int syntheticProvenanceSize() {
    return syntheticProvenanceSize;
  }

  public int syntheticSourceParallelism() {
    return syntheticSourceParallelism;
  }

  public int syntheticProvenanceOverlap() {
    Validate.isTrue(syntheticProvenanceOverlap >= 0);
    Validate.isTrue(syntheticProvenanceOverlap < syntheticProvenanceSize);
    return syntheticProvenanceOverlap;
  }

  public ProvenanceActivator genealogActivator() {
    return provenanceActivator;
  }

  public boolean graphTraversalStatistics() {
    return traversalStatistics;
  }

  public int syntheticTupleSize() {
    return this.syntheticTupleSize;
  }

  public ProvenanceGraphEncoder newGraphEncoder(String name, int subtaskIndex) {

    if (FileProvenanceGraphEncoder.class.getSimpleName().equals(graphEncoder)) {
      return new FileProvenanceGraphEncoder(outputFile(subtaskIndex, name), autoFlush);
    } else if (TimestampedFileProvenanceGraphEncoder.class.getSimpleName().equals(graphEncoder)) {
      return new TimestampedFileProvenanceGraphEncoder(outputFile(subtaskIndex, name), autoFlush);
    } else if (GephiProvenanceGraphEncoder.class.getSimpleName().equals(graphEncoder)) {
      return new GephiProvenanceGraphEncoder("workspace1");
    } else if (NoOpProvenanceGraphEncoder.class.getSimpleName().equals(graphEncoder)) {
      return new NoOpProvenanceGraphEncoder();
    } else {
      throw new IllegalArgumentException(String.format("Invalid graph encoder: %s", graphEncoder));
    }

  }

  public boolean disableSinkChaining() {
    return disableSinkChaining;
  }

  public long pollFrequencyMillis() {
    return pollFrequencyMillis;
  }

  public boolean uniqueSqlKeys() {
    return uniqueDbKeys;
  }

  private static class AggregateStrategyConverter
      implements IStringConverter<Supplier<ProvenanceAggregateStrategy>> {

    @Override
    public Supplier<ProvenanceAggregateStrategy> convert(String s) {
      switch (s.trim()) {
        case "unsortedPtr":
          return (Supplier<ProvenanceAggregateStrategy> & Serializable)
              UnsortedPointersAggregateStrategy::new;
        case "sortedPtr":
          return (Supplier<ProvenanceAggregateStrategy> & Serializable)
              SortedPointersAggregateStrategy::new;
        case "list":
          return (Supplier<ProvenanceAggregateStrategy> & Serializable) ListAggregateStrategy::new;
        default:
          throw new IllegalArgumentException("Unknown GeneaLog aggregate strategy provided");
      }
    }
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("statisticsFolder", statisticsFolder)
        .append("inputFile", inputFile)
        .append("outputFile", outputFile)
        .append("sourcesNumber", sourcesNumber)
        .append("autoFlush", autoFlush)
        .append("sinkParallelism", sinkParallelism)
        .append("distributed", distributed)
        .append("traversalStatistics", traversalStatistics)
        .append("sourceRepetitions", sourceRepetitions)
        .append("idShift", idShift)
        .append("sourceIP", sourceIP)
        .append("sourcePort", sourcePort)
        .append("maxParallelism", maxParallelism)
        .append("provenanceActivator", provenanceActivator)
        .append("aggregateStrategySupplier", aggregateStrategySupplier)
        .append("graphEncoder", graphEncoder)
        .append("watermarkInterval", watermarkInterval)
        .append("syntheticInputLength", syntheticInputLength)
        .append("syntheticDelay", syntheticDelay)
        .append("syntheticProvenanceSize", syntheticProvenanceSize)
        .append("syntheticTupleSize", syntheticTupleSize)
        .append("syntheticSourceParallelism", syntheticSourceParallelism)
        .append("syntheticProvenanceOverlap", syntheticProvenanceOverlap)
        .append("disableSinkChaining", disableSinkChaining)
        .toString();
  }
}
