package io.palyvos.provenance.ananke.output;

import io.palyvos.provenance.util.TimestampedUIDTuple;

public class TimestampedFileProvenanceGraphEncoder extends FileProvenanceGraphEncoder {

  private static final int SINK_ORDER = 0;
  private static final int SOURCE_ORDER = 1;
  private static final int EDGE_ORDER = 2;
  private static final int ACK_ORDER = 3;

  public TimestampedFileProvenanceGraphEncoder(String outputFile, boolean autoFlush) {
    super(outputFile, autoFlush);
  }

  @Override
  public void sourceVertex(TimestampedUIDTuple tuple, long streamTimestamp, long dataTimestamp) {
    writer.format("%d %d %d ", streamTimestamp, SOURCE_ORDER, dataTimestamp);
    super.sourceVertex(tuple, streamTimestamp, dataTimestamp);
  }

  @Override
  public void sinkVertex(TimestampedUIDTuple tuple, long streamTimestamp, long dataTimestamp) {
    writer.format("%d %d %d ", streamTimestamp, SINK_ORDER, dataTimestamp);
    super.sinkVertex(tuple, streamTimestamp, dataTimestamp);
  }

  @Override
  public void edge(long sourceUID, long sinkUID, long timestamp) {
    writer.format("%d %d %d ", timestamp, EDGE_ORDER, timestamp);
    super.edge(sourceUID, sinkUID, timestamp);
  }

  @Override
  public void ack(long uid, long timestamp) {
    writer.format("%d %d %d ", timestamp, ACK_ORDER, timestamp);
    super.ack(uid, timestamp);
  }
}
