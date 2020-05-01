package io.palyvos.provenance.util;

import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ParallelTextSource<T extends BaseTuple>
    extends RichParallelSourceFunction<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelTextSource.class);
  private static final String DEFAULT_NAME = "SOURCE";
  private final ExperimentSettings settings;
  private String inputFile;
  private transient CountStat throughputStatistic;
  private volatile boolean enabled;
  private final String name;

  public ParallelTextSource(ExperimentSettings settings) {
    this(DEFAULT_NAME, settings);
  }

  public ParallelTextSource(String name, ExperimentSettings settings) {
    this.settings = settings;
    this.name = name;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    RuntimeContext ctx = getRuntimeContext();
    this.inputFile = settings.inputFile();
    LOG.info("Source {} reading from file {}", ctx.getIndexOfThisSubtask(), inputFile);
    this.throughputStatistic =
        new CountStat(
            settings.throughputFile(name, ctx.getIndexOfThisSubtask()),
            settings.autoFlush());
    enabled = true;
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    int repetition = 0;
    long highestTimestamp = 0;
    int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
    long idShift = settings.idShift() * taskIndex;
    while (enabled && repetition < settings.sourceRepetitions()) {
      long timestampShift = repetition == 0 ? 0 : highestTimestamp + 1;
      LOG.info(
          "Source {} repetition {} (timestamp shift = {}, id shift = {})",
          taskIndex,
          repetition,
          timestampShift,
          idShift);
      try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
        String line = br.readLine();
        while (enabled && line != null) {
          throughputStatistic.increase(1);
          T tuple = getTuple(line.trim(), taskIndex, idShift);
          long newTimestamp = tuple.getTimestamp() + timestampShift;
          tuple.setTimestamp(newTimestamp);
          highestTimestamp = Math.max(highestTimestamp, newTimestamp);
          ctx.collect(tuple);
          line = br.readLine();
        }
      }
      repetition += 1;
    }
    LOG.info("Source {} terminating...", taskIndex);
    throughputStatistic.close();
  }

  protected abstract T getTuple(String line, int taskIndex, long idShift);

  @Override
  public void cancel() {
    enabled = false;
  }
}
