package io.palyvos.provenance.ananke.functions;

import io.palyvos.provenance.util.TimestampedUIDTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.UIDAssigner;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * ProvenanceInitializer for cases where source tuples already implement {@link TimestampedUIDTuple}.
 * <b>WARNING:</b> Applied after the tuples have been assigned a UID using a {@link UIDAssigner}.
 * <b>WARNING:</b> Will not work if first provenance node is aggregate!
 * @param <T> The type of source tuples.
 */
public class ProvenanceInitializerAssisted<T extends TimestampedUIDTuple>
    implements MapFunction<T, ProvenanceTupleContainer<T>> {

  @Override
  public ProvenanceTupleContainer<T> map(T value) {
    ProvenanceTupleContainer<T> out = new ProvenanceTupleContainer<>(value);
    out.initGenealog(GenealogTupleType.META_SOURCE);
    return out;
  }
}
