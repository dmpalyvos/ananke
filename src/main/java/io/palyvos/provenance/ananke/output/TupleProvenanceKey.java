package io.palyvos.provenance.ananke.output;

import io.palyvos.provenance.util.TimestampedUIDTuple;
import io.palyvos.provenance.util.TimestampConverter;
import java.util.Objects;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

class TupleProvenanceKey extends ProvenanceKey {

  final long uid;
  private final long convertedTimestamp;
  private final long stimulus;

  TupleProvenanceKey(TimestampedUIDTuple tuple, TimestampConverter timestampConverter,
      long stimulus) {
    Validate.notNull(tuple);
    this.uid = tuple.getUID();
    this.convertedTimestamp = timestampConverter.apply(tuple.getTimestamp());
    Validate.isTrue(convertedTimestamp >= 0, "Negative timestamp!");
    this.stimulus = stimulus;
  }

  @Override
  long timestamp() {
    return convertedTimestamp;
  }

  @Override
  long stimulus() {
    return stimulus;
  }

  @Override
  long tieBreaker() {
    return uid;
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
    return uid == that.uid &&
        convertedTimestamp == that.convertedTimestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(uid, convertedTimestamp);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("uid", uid)
        .append("convertedTimestamp", convertedTimestamp)
        .toString();
  }
}
