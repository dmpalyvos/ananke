package io.palyvos.provenance.ananke.output;

import io.palyvos.provenance.util.IncreasingUIDGenerator;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import org.apache.commons.lang3.Validate;

public class FileProvenanceGraphEncoder implements ProvenanceGraphEncoder, Serializable {

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
    writer.println(vertexString(tuple, "SOURCE"));
  }

  @Override
  public void sinkVertex(TimestampedUIDTuple tuple, long streamTimestamp, long dataTimestamp) {
    writer.println(vertexString(tuple, "SINK"));
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

  private String vertexString(TimestampedUIDTuple tuple, String type) {
    Validate.notNull(tuple);
    return String.format(
        "%s ::: %s ::: %s", type, formattedUID(tuple.getUID()), tuple);
  }

  private String formattedUID(long uid) {
    return String.format("{%s}", IncreasingUIDGenerator.asString(uid));
  }
}
