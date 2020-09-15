package io.palyvos.provenance.util;

import io.palyvos.provenance.ananke.output.ProvenanceGraphEncoder;
import io.palyvos.provenance.genealog.GenealogTuple;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;

/**
 * GraphEncoder that stores all changes and manually commits or aborts at a later point. Does NOT
 * support timestamps.
 */
class TransactionalProvenanceGraphEncoder implements ProvenanceGraphEncoder {

  private final long TIMESTAMP_VALUE = -1;
  private final ProvenanceGraphEncoder encoder;
  private final List<Vertice> sources = new ArrayList<>();
  private final List<Vertice> sinks = new ArrayList<>();
  private final List<Edge> edges = new ArrayList<>();
  private final List<Long> acks = new ArrayList<>();

  private static class Vertice {

    private final long uid;
    private final String tuple;

    public Vertice(long uid, String tuple) {
      this.uid = uid;
      this.tuple = tuple;
    }
  }

  private static class Edge {

    private final long source;
    private final long sink;

    public Edge(long source, long sink) {
      this.source = source;
      this.sink = sink;
    }
  }


  public TransactionalProvenanceGraphEncoder(ProvenanceGraphEncoder encoder) {
    Validate.notNull(encoder);
    this.encoder = encoder;
  }

  @Override
  public void sourceVertex(long uid, String tuple) {
    sources.add(new Vertice(uid, tuple));
  }

  @Override
  public void sinkVertex(long uid, String tuple) {
    sinks.add(new Vertice(uid, tuple));
  }

  @Override
  public void edge(long sourceUID, long sinkUID, long timestamp) {
    edges.add(new Edge(sourceUID, sinkUID));
  }

  @Override
  public void ack(long uid, long timestamp) {
    acks.add(uid);
  }

  public void commit() {
    for (Vertice sink : sinks) {
      encoder.sinkVertex(sink.uid, sink.tuple);
    }
    for (Vertice source : sources) {
      encoder.sourceVertex(source.uid, source.tuple);
    }
    for (Edge edge : edges) {
      encoder.edge(edge.source, edge.sink, TIMESTAMP_VALUE);
    }
    for (long ack : acks) {
      encoder.ack(ack, TIMESTAMP_VALUE);
    }
    reset();
  }

  public void reset() {
    sources.clear();
    sinks.clear();
    edges.clear();
    acks.clear();
  }

  @Override
  public void debug(String message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    encoder.close();
  }

  @Override
  public void sourceVertex(TimestampedUIDTuple tuple, long streamTimestamp, long dataTimestamp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void ack(TimestampedUIDTuple tuple, long timestamp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void edge(TimestampedUIDTuple sourceTuple,
      GenealogTuple sinkTuple, long timestamp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sinkVertex(TimestampedUIDTuple tuple, long streamTimestamp, long dataTimestamp) {
    throw new UnsupportedOperationException();
  }
}
