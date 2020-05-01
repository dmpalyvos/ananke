package io.palyvos.provenance.usecases.synthetic.provenance;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.CountStat;
import io.palyvos.provenance.util.IncreasingUIDGenerator;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import io.palyvos.provenance.genealog.GenealogTuple;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source that emits sink tuples which already contain (random) provenance.
 *
 * @param <T> The type of sink tuples to be emitted.
 */
public class SyntheticProvenanceSource<T extends GenealogTuple> extends
    RichParallelSourceFunction<T> {

  private static final String NAME = "SOURCE";
  private static final Logger LOG = LoggerFactory.getLogger(SyntheticProvenanceSource.class);
  private static final double ALLOWED_OVERLAP_DEVIATION_PERCENT = 5;
  private final ExperimentSettings settings;
  private transient CountStat throughputStatistic;
  private volatile boolean enabled;
  private final List<T> sinkTuples = new ArrayList<>();
  private final Supplier<T> tupleSupplier;
  private final Supplier<? extends TimestampedUIDTuple> provenanceSupplier;
  private transient IncreasingUIDGenerator uidGenerator;
  private transient Random random;

  private transient AverageAccumulator realProvenanceOverlap;

  public <F extends Supplier<T> & Serializable> SyntheticProvenanceSource(
      F tupleSupplier,
      Supplier<? extends TimestampedUIDTuple> provenanceSupplier,
      ExperimentSettings settings) {
    this.settings = settings;
    this.tupleSupplier = tupleSupplier;
    this.provenanceSupplier = provenanceSupplier;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    final int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
    this.throughputStatistic =
        new CountStat(settings.throughputFile(NAME, subtaskIndex), settings.autoFlush());
    initData(subtaskIndex, numberOfParallelSubtasks);
  }

  void initData(int subtaskIndex, int numberOfParallelSubtasks) {
    this.uidGenerator = new IncreasingUIDGenerator(subtaskIndex);
    this.realProvenanceOverlap = new AverageAccumulator();
    this.random = new Random(subtaskIndex);
    LOG.info("Initializing tuples of source {}", subtaskIndex);
    Collection<TimestampedUIDTuple> previousProvenance = Collections.emptySet();
    final int inputSize =
        settings.syntheticInputSize() / numberOfParallelSubtasks;
    for (int i = 0; i < inputSize; i++) {
      final int timestamp = settings.syntheticDelay() + i;
      T sinkTuple = newSinkTuple(timestamp, previousProvenance);
      sinkTuples.add(sinkTuple);
      previousProvenance = sinkTuple.getProvenance();
    }
    LOG.info(
        "Loaded {} tuples for source {}",
        settings.syntheticInputSize(),
        subtaskIndex);
    validateProvenanceOverlap();
  }

  T newSinkTuple(long timestamp, Collection<TimestampedUIDTuple> unsortedProvenance) {
    // Sort provenance for consistency between executions
    final List<TimestampedUIDTuple> sortedProvenance = new ArrayList<>(unsortedProvenance);
    Collections.sort(sortedProvenance, Comparator.comparing(TimestampedUIDTuple::getUID));
    Collections.shuffle(sortedProvenance, random);
    final T sinkTuple = tupleSupplier.get();
    sinkTuple.setTimestamp(timestamp);
    final Collection<TimestampedUIDTuple> provenance = new HashSet<>();
    for (TimestampedUIDTuple pTuple : sortedProvenance) {
      if (provenance.size() >= settings.syntheticProvenanceOverlap()) {
        break;
      }
      if (timestamp - pTuple.getTimestamp() <= settings.syntheticDelay()) {
        provenance.add(pTuple);
      }
    }
    realProvenanceOverlap.add(provenance.size());
    final int remainingProvenanceSize =
        Math.max(0, settings.syntheticProvenanceSize() - provenance.size());
    for (int j = 0; j < remainingProvenanceSize; j++) {
      final TimestampedUIDTuple sourceTuple = provenanceSupplier.get();
      sourceTuple.setStimulus(System.currentTimeMillis());
      sourceTuple.setTimestamp(
          sinkTuple.getTimestamp() - random.nextInt(settings.syntheticDelay()));
      sourceTuple.setUID(uidGenerator.newUID());
      provenance.add(sourceTuple);
    }
    sinkTuple.getGenealogData().setProvenance(provenance);
    return sinkTuple;
  }

  private void validateProvenanceOverlap() {
    if (settings.syntheticProvenanceOverlap() == 0) {
      return;
    }
    LOG.info(
        "Requested Overlap: {} | Average overlap: {}",
        settings.syntheticProvenanceOverlap(),
        realProvenanceOverlap.getLocalValue());
    double overlapRelativeDiff =
        100
            * (realProvenanceOverlap.getLocalValue() - settings.syntheticProvenanceOverlap())
            / settings.syntheticProvenanceOverlap();
    Validate.isTrue(
        Math.abs(overlapRelativeDiff) <= ALLOWED_OVERLAP_DEVIATION_PERCENT,
        "Provenance overlap deviation is too high: %3.2f%% (Maximum allowed: %3.2f%%)",
        Math.abs(overlapRelativeDiff),
        ALLOWED_OVERLAP_DEVIATION_PERCENT);
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    enabled = true;
    Iterator<T> it = sinkTuples.iterator();
    while (enabled && it.hasNext()) {
      final T tuple = it.next();
      tuple.setStimulus(System.currentTimeMillis());
      ctx.collect(tuple);
      throughputStatistic.increase(1);
    }
    enabled = false;
    throughputStatistic.close();
  }

  @Override
  public void cancel() {
    enabled = false;
  }
}
