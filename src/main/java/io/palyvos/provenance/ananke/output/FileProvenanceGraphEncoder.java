package io.palyvos.provenance.ananke.output;

import io.palyvos.provenance.util.IncreasingUIDGenerator;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import org.apache.commons.lang3.Validate;

public class FileProvenanceGraphEncoder implements ProvenanceGraphEncoder, Serializable {

  public static final String TYPE_SOURCE = "SOURCE";
  public static final String TYPE_SINK = "SINK";
  protected final PrintWriter writer;

  public FileProvenanceGraphEncoder(String outputFile, boolean autoFlush) {
    try {
      writer = new PrintWriter(new FileWriter(outputFile, autoFlush));
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void sourceVertex(TimestampedUIDTuple tuple, long streamTimestamp, long dataTimestamp) {
    writer.println(vertexString(tuple, TYPE_SOURCE));
  }

  @Override
  public void sourceVertex(long uid, String tuple) {
    writer.println(vertexString(uid, tuple, TYPE_SOURCE));
  }

  @Override
  public void sinkVertex(TimestampedUIDTuple tuple, long streamTimestamp, long dataTimestamp) {
    writer.println(vertexString(tuple, TYPE_SINK));
  }

  @Override
  public void sinkVertex(long uid, String tuple) {
    writer.println(vertexString(uid, tuple, TYPE_SINK));
  }

  @Override
  public void edge(long sourceUID, long sinkUID, long timestamp) {
    writer.format(
        "EDGE ::: %s -> %s\n",
        formattedUID(sourceUID), formattedUID(sinkUID));
  }

  @Override
  public void ack(long uid, long timestamp) {
    writer.format("ACK ::: %s\n", formattedUID(uid));
  }

  @Override
  public void debug(String message) {
    writer.println(message);
  }

  @Override
  public void close() {
    writer.flush();
    writer.close();
  }

  public String vertexString(TimestampedUIDTuple tuple, String type) {
    Validate.notNull(tuple);
    return vertexString(tuple.getUID(), String.valueOf(tuple), type);
  }

  private String vertexString(long uid, String tuple, String type) {
    return String.format("%s ::: %s ::: %s", type, formattedUID(uid), tuple);
  }

  private String formattedUID(long uid) {
    return String.format("{%s}", IncreasingUIDGenerator.asString(uid));
  }
}
