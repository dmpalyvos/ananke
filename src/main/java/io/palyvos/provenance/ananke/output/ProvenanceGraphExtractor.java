package io.palyvos.provenance.ananke.output;

import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.IncreasingUIDGenerator;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import io.palyvos.provenance.util.TimestampConverter;
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

  private static Logger LOG = LoggerFactory.getLogger(ProvenanceGraphExtractor.class);
  private final TimestampConverter timestampConverter;
  private final long maxDelayMillis;
  private NavigableSet<ProvenanceKey> pendingSourceAcks;
  private final GenealogGraphTraverser genealogGraphTraverser;

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
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    LOG.warn("Unique IDs required in the tuples for forward provenance to work correctly! See: {}",
        IncreasingUIDGenerator.class.getName());
    super.open(parameters);
    pendingSourceAcks = new TreeSet<>();
  }

  @Override
  public void flatMap(T tuple, Collector<ProvenanceGraphNode> out) throws Exception {
    Collection<TimestampedUIDTuple> provenance = genealogGraphTraverser.getProvenance(tuple);
    out.collect(ProvenanceGraphNode.newSink(tuple));
    for (TimestampedUIDTuple sourceTuple : provenance) {
      ProvenanceKey sourceTupleKey = TupleProvenanceKey.ofUID(sourceTuple, timestampConverter);
      if (!pendingSourceAcks.contains(sourceTupleKey)) {
        out.collect(ProvenanceGraphNode.newSource(sourceTuple));
        pendingSourceAcks.add(sourceTupleKey);
      }
      out.collect(ProvenanceGraphNode.newEdge(sourceTuple.getUID(), tuple.getUID()));
    }
    out.collect(ProvenanceGraphNode.newACK(tuple.getUID()));
  }

  /**
   * Create a <b>Serializable</b> {@link Consumer} for each watermark that calls updateProvenance.
   *
   * @return The watermark consumer for this object.
   */
  public BiConsumer<Watermark, Collector<ProvenanceGraphNode>> watermarkConsumer() {
    return (Serializable & BiConsumer<Watermark, Collector<ProvenanceGraphNode>>)
        (watermark, out) -> {
          updateAndEmitProvenance(watermark, out);
        };
  }

  public void updateAndEmitProvenance(Watermark currentWatermark,
      Collector<ProvenanceGraphNode> out) {
    ProvenanceKey currentTimeKey = inactiveDataBoundary(currentWatermark);
    Iterator<ProvenanceKey> it = pendingSourceAcks.iterator();
    while (it.hasNext()) {
      UIDProvenanceKey key = (UIDProvenanceKey) it.next();
      if (Objects.compare(key, currentTimeKey, ProvenanceKey::compareTo) >= 0) {
        break;
      }
      out.collect(ProvenanceGraphNode.newACK(key.uid));
      it.remove();
    }
  }

  private ProvenanceKey inactiveDataBoundary(Watermark currentWatermark) {
    // Outputs with timestamps lower than this are ready to be processed
    long inactiveBoundaryTs = Math.max(0, currentWatermark.getTimestamp() - maxDelayMillis - 1);
    return TupleProvenanceKey.ofTimestamp(inactiveBoundaryTs);
  }

}
