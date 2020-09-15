package io.palyvos.provenance.ananke.output;

import io.palyvos.provenance.util.TimestampConverter;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.util.Comparator;
import org.apache.commons.lang3.Validate;

abstract class ProvenanceKey implements Comparable<ProvenanceKey> {

  private static final Comparator keyComparator =
      Comparator.comparingLong(ProvenanceKey::timestamp).thenComparing(ProvenanceKey::tieBreaker);

  public static ProvenanceKey ofTuple(
      TimestampedUIDTuple sourceTuple, TimestampConverter timestampConverter, long stimulus) {
    return new TupleProvenanceKey(sourceTuple, timestampConverter, stimulus);
  }

  public static ProvenanceKey ofTimestamp(long convertedTimestamp) {
    Validate.isTrue(convertedTimestamp >= 0, "Negative timestamp");
    return new TimestampProvenanceKey(convertedTimestamp);
  }

  abstract long timestamp();

  abstract long stimulus();

  abstract long tieBreaker();

  @Override
  public int compareTo(ProvenanceKey o) {
    return keyComparator.compare(this, o);
  }
}
