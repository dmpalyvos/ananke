package io.palyvos.provenance.ananke.output;

import org.apache.commons.lang3.Validate;

class TimestampProvenanceKey extends ProvenanceKey {

  private final long timestamp;

  TimestampProvenanceKey(long timestamp) {
    Validate.isTrue(timestamp >= 0, "Negative timestamp!");
    this.timestamp = timestamp;
  }

  @Override
  long timestamp() {
    return timestamp;
  }

  @Override
  long stimulus() {
    return Long.MAX_VALUE;
  }

  @Override
  long tieBreaker() {
    return Long.MAX_VALUE;
  }
}
