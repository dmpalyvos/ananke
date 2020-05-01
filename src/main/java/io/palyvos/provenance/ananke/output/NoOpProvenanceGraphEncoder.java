package io.palyvos.provenance.ananke.output;

import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.io.Serializable;

public class NoOpProvenanceGraphEncoder implements ProvenanceGraphEncoder, Serializable {

  @Override
  public void sourceVertex(TimestampedUIDTuple tuple, long streamTimestamp, long dataTimestamp) {
  }

  @Override
  public void sinkVertex(TimestampedUIDTuple tuple, long streamTimestamp, long dataTimestamp) {
  }

  @Override
  public void edge(long sourceUID, long sinkUID, long timestamp) {
  }

  @Override
  public void ack(long uid, long timestamp) {
  }

  @Override
  public void close() {
  }
}
