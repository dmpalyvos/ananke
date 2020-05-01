package io.palyvos.provenance.ananke.output;

import io.palyvos.provenance.util.TimestampedUIDTuple;

public class ProvenanceGraphNode {

  public ProvenanceGraphNodeType type;
  public TimestampedUIDTuple tuple;
  public Long uid1;
  public Long uid2;

  public static ProvenanceGraphNode newSource(TimestampedUIDTuple sourceTuple) {
    return new ProvenanceGraphNode(ProvenanceGraphNodeType.SOURCE, sourceTuple, null, null);
  }

  public static ProvenanceGraphNode newSink(TimestampedUIDTuple sinkTuple) {
    return new ProvenanceGraphNode(ProvenanceGraphNodeType.SINK, sinkTuple, null, null);
  }

  public static ProvenanceGraphNode newEdge(long sourceUID, long sinkUID) {
    return new ProvenanceGraphNode(ProvenanceGraphNodeType.EDGE, null, sourceUID, sinkUID);
  }

  public static ProvenanceGraphNode newACK(long uid) {
    return new ProvenanceGraphNode(ProvenanceGraphNodeType.ACK, null, uid, null);
  }

  public ProvenanceGraphNode(ProvenanceGraphNodeType type,
      TimestampedUIDTuple tuple, Long uid1, Long uid2) {
    this.type = type;
    this.tuple = tuple;
    this.uid1 = uid1;
    this.uid2 = uid2;
  }

  public ProvenanceGraphNode() {
    //Makes class POJO, allows automatic serialization by Flink's PojoSerializer
  }

  public long sourceUID() {
    return uid1;
  }

  public long sinkUID() {
    return uid2;
  }

  public long ackUID() {
    return uid1;
  }

  public TimestampedUIDTuple sourceTuple() {
    return tuple;
  }

  public TimestampedUIDTuple sinkTuple() {
    return tuple;
  }

  @Override
  public String toString() {
    return "ProvenanceGraphNode{" +
        "type=" + type +
        ", tuple=" + tuple +
        ", uid1=" + uid1 +
        ", uid2=" + uid2 +
        '}';
  }
}
