package io.palyvos.provenance.ananke.output;

import io.palyvos.provenance.util.TimestampedUIDTuple;
import io.palyvos.provenance.util.TimestampConverter;
import java.util.Objects;
import org.apache.commons.lang3.Validate;

class TupleProvenanceKey extends ProvenanceKey {

  private final TimestampedUIDTuple tuple;
  private final long convertedTimestamp;

  TupleProvenanceKey(TimestampedUIDTuple tuple, TimestampConverter timestampConverter) {
    Validate.notNull(tuple);
    this.tuple = tuple;
    this.convertedTimestamp = timestampConverter.apply(tuple.getTimestamp());
    Validate.isTrue(convertedTimestamp >= 0, "Negative timestamp!");
  }

  @Override
  long timestamp() {
    return convertedTimestamp;
  }

  @Override
  long tieBreaker() {
    return tuple.getUID();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TupleProvenanceKey that = (TupleProvenanceKey) o;
    return Objects.equals(tuple.getUID(), that.tuple.getUID());
  }

  @Override
  public int hashCode() {
    return Objects.hash(tuple);
  }

  @Override
  public String toString() {
//    return String.format("{uid: %d, ts: %d} | %s", tuple.getUID(), convertedTimestamp, tuple);
    return String.format("{uid: %d} | %s", tuple.getUID(), tuple);
  }
}
