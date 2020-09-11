package io.palyvos.provenance.ananke.output;

import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.util.AvgStat;
import io.palyvos.provenance.util.CountStat;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.IncreasingUIDGenerator;
import io.palyvos.provenance.util.MaxStat;
import io.palyvos.provenance.util.TimestampConverter;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProvenanceGraphExtractor<T extends GenealogTuple> extends
    RichFlatMapFunction<T, ProvenanceGraphNode>
    implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ProvenanceGraphExtractor.class);
  public static final String EXTRACTOR_STAT_NAME = "ANANKE";
  private final ExperimentSettings settings;
  private final TimestampConverter timestampConverter;
  private final long maxDelayMillis;
  private NavigableSet<ProvenanceKey> pendingSourceAcks;
  private final GenealogGraphTraverser genealogGraphTraverser;
  private transient AvgStat provenanceWriteTimeStatistic;
  private transient AvgStat provenanceReadTimeStatistic;
  private transient CountStat provenanceReadsStatistic;
  private transient CountStat provenanceWritesStatistic;
  private transient MaxStat deliveryLatencyStatistic;


  public ProvenanceGraphExtractor(
      ExperimentSettings settings, long maxDelayMillis, TimestampConverter timestampConverter) {
    this(maxDelayMillis, timestampConverter, settings);
  }

  public ProvenanceGraphExtractor(
      long maxDelayMillis,
      TimestampConverter timestampConverter,
      ExperimentSettings settings) {
    Validate.isTrue(maxDelayMillis >= 0);
    Validate.notNull(timestampConverter);
    Validate.notNull(settings);
    this.maxDelayMillis = maxDelayMillis;
    this.timestampConverter = timestampConverter;
    genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    this.settings = settings;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    LOG.warn("Unique IDs required in the tuples for forward provenance to work correctly! See: {}",
        IncreasingUIDGenerator.class.getName());
    final int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
    provenanceWriteTimeStatistic =
        new AvgStat(settings.provenanceWriteTimeFile(indexOfThisSubtask, EXTRACTOR_STAT_NAME),
            settings.autoFlush());
    this.provenanceReadTimeStatistic =
        new AvgStat(settings.provenanceReadTimeFile(indexOfThisSubtask, EXTRACTOR_STAT_NAME),
            settings.autoFlush());
    provenanceWritesStatistic = new CountStat(
        settings.provenanceWritesFile(indexOfThisSubtask, EXTRACTOR_STAT_NAME),
        settings.autoFlush());
    provenanceReadsStatistic = new CountStat(
        settings.provenanceReadsFile(indexOfThisSubtask, EXTRACTOR_STAT_NAME),
        settings.autoFlush());
    deliveryLatencyStatistic = new MaxStat(
        settings.deliveryLatencyFile(indexOfThisSubtask, EXTRACTOR_STAT_NAME),
        settings.autoFlush());
    pendingSourceAcks = new TreeSet<>();
    super.open(parameters);
  }

  @Override
  public void close() throws Exception {
    provenanceReadTimeStatistic.close();
    provenanceWriteTimeStatistic.close();
    provenanceReadsStatistic.close();
    provenanceWritesStatistic.close();
    super.close();
  }

  @Override
  public void flatMap(T tuple, Collector<ProvenanceGraphNode> out) {
    Collection<TimestampedUIDTuple> provenance = genealogGraphTraverser.getProvenance(tuple);
    final long start = System.currentTimeMillis();
    out.collect(ProvenanceGraphNode.newSink(tuple));
    for (TimestampedUIDTuple sourceTuple : provenance) {
      ProvenanceKey sourceTupleKey = ProvenanceKey.ofTuple(sourceTuple, timestampConverter, start);
      if (!pendingSourceAcks.contains(sourceTupleKey)) {
        out.collect(ProvenanceGraphNode.newSource(sourceTuple));
        pendingSourceAcks.add(sourceTupleKey);
      }
      out.collect(ProvenanceGraphNode.newEdge(sourceTuple.getUID(), tuple.getUID()));
    }
    out.collect(ProvenanceGraphNode.newACK(tuple.getUID()));
    final long end = System.currentTimeMillis();
    provenanceWriteTimeStatistic.add(end - start);
    deliveryLatencyStatistic.add(end - start);
    // NOTE: Writes will be more than reality for provenance overlap > 1
    // But this implementation is consistent with the DB implementations
    // "Wrote" 1 sink  + 1 sink ACK + P sources + P edges + P ACKs
    provenanceWritesStatistic.increase(2 + (3 * provenance.size()));
    // "Read" (outputted) 1 sink + 1 sink ACK  + P sources + P edges
    provenanceReadsStatistic.increase(2 + (2 * provenance.size()));
  }

  /**
   * Create a <b>Serializable</b> {@link Consumer} for each watermark that calls updateProvenance.
   *
   * @return The watermark consumer for this object.
   */
  public BiConsumer<Watermark, Collector<ProvenanceGraphNode>> watermarkConsumer() {
    return (Serializable & BiConsumer<Watermark, Collector<ProvenanceGraphNode>>)
        this::updateAndEmitProvenance;
  }

  public void updateAndEmitProvenance(Watermark currentWatermark,
      Collector<ProvenanceGraphNode> out) {
    final long start = System.currentTimeMillis();
    ProvenanceKey currentTimeKey = inactiveDataBoundary(currentWatermark);
    Iterator<ProvenanceKey> it = pendingSourceAcks.iterator();
    boolean emitted = false;
    while (it.hasNext()) {
      TupleProvenanceKey key = (TupleProvenanceKey) it.next();
      if (Objects.compare(key, currentTimeKey, ProvenanceKey::compareTo) >= 0) {
        break;
      }
      out.collect(ProvenanceGraphNode.newACK(key.uid));
      it.remove();
      // Read (outputted) 1 ack
      provenanceReadsStatistic.increase(1);
      emitted = true;
    }
    final long end = System.currentTimeMillis();
    if (emitted) {
      deliveryLatencyStatistic.add(end - start);
    }
    provenanceReadTimeStatistic.add(end - start);
  }

  private ProvenanceKey inactiveDataBoundary(Watermark currentWatermark) {
    // Outputs with timestamps lower than this are ready to be processed
    long inactiveBoundaryTs = Math.max(0, currentWatermark.getTimestamp() - maxDelayMillis);
    return ProvenanceKey.ofTimestamp(inactiveBoundaryTs);
  }

}
