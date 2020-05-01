package io.palyvos.provenance.ananke.output;

import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import org.apache.commons.lang3.Validate;

public interface ProvenanceGraphEncoder {
  void sourceVertex(TimestampedUIDTuple tuple, long streamTimestamp, long dataTimestamp);

  void sinkVertex(TimestampedUIDTuple tuple, long streamTimestamp, long dataTimestamp);

  default void edge(TimestampedUIDTuple sourceTuple, GenealogTuple sinkTuple, long timestamp) {
    Validate.notNull(sourceTuple);
    Validate.notNull(sinkTuple);
    edge(sourceTuple.getUID(), sinkTuple.getUID(), timestamp);
  }

  void edge(long sourceUID, long sinkUID, long timestamp);

  void ack(long uid, long timestamp);

  default void ack(TimestampedUIDTuple tuple, long timestamp) {
    Validate.notNull(tuple);
    ack(tuple.getUID(), timestamp);
  }

  default void debug(String message) {

  }

  void close();
}
