package io.palyvos.provenance.ananke.stdops;

import io.palyvos.provenance.util.TimestampedTuple;
import java.io.Serializable;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import io.palyvos.provenance.ananke.output.ProvenanceGraphNodeType;
import io.palyvos.provenance.ananke.functions.CustomGenericSerializer;

public class HelperProvenanceGraphTuple implements TimestampedTuple {

  public long timestamp;
  public long rightBoundary;
  public ProvenanceGraphNodeType type;
  public TimestampedUIDTuple provenanceTuple;
  public long uid1;
  public long uid2; // Set only for edges
  public long count;
  public boolean valid;

  public static HelperProvenanceGraphTuple newSourceVertex(long timestamp,
      TimestampedUIDTuple provenanceTuple, long uid) {
    return new HelperProvenanceGraphTuple(timestamp, -1, ProvenanceGraphNodeType.SOURCE,
        provenanceTuple, uid, -1, 0, true);
  }

  public static HelperProvenanceGraphTuple newSinkVertex(long timestamp,
      TimestampedUIDTuple provenanceTuple, long uid) {
    return new HelperProvenanceGraphTuple(timestamp, -1, ProvenanceGraphNodeType.SINK,
        provenanceTuple, uid, -1, 0, true);
  }

  public static HelperProvenanceGraphTuple newEdge(long timestamp, long uid1, long uid2) {
    return new HelperProvenanceGraphTuple(timestamp, -1, ProvenanceGraphNodeType.EDGE, null, uid1,
        uid2, 0, true);
  }

  public static HelperProvenanceGraphTuple newACK(long timestamp, long uid) {
    return new HelperProvenanceGraphTuple(timestamp, -1, ProvenanceGraphNodeType.ACK, null, uid, -1,
        0, true);
  }

  public HelperProvenanceGraphTuple(HelperProvenanceGraphTuple other, boolean valid) {
    this.timestamp = other.timestamp;
    this.rightBoundary = other.rightBoundary;
    this.type = other.type;
    this.provenanceTuple = other.provenanceTuple;
    this.uid1 = other.uid1;
    this.uid2 = other.uid2;
    this.count = other.count;
    this.valid = valid;
  }

  public HelperProvenanceGraphTuple(long timestamp, long rightBoundary,
      ProvenanceGraphNodeType type, TimestampedUIDTuple provenanceTuple, long uid1, long uid2,
      long count, boolean valid) {
    this.timestamp = timestamp;
    this.rightBoundary = rightBoundary;
    this.type = type;
    this.provenanceTuple = provenanceTuple;
    this.uid1 = uid1;
    this.uid2 = uid2;
    this.count = count;
    this.valid = valid;
  }

  public HelperProvenanceGraphTuple() {}

  @Override
  public String toString() {
    return "ProvenanceGraphTuple [timestamp=" + timestamp + ", rightBoundary=" + rightBoundary
        + ", type=" + type + ", provenanceTuple=" + provenanceTuple + ", uid1=" + uid1 + ", uid2="
        + uid2 + ", count=" + count + ", valid=" + valid + "]";
  }

  public long getCount() {
    return count;
  }

  public String getUID() {
    if (uid1 == -1) {
      throw new IllegalStateException("uid1 cannot be unset");
    }
    if (uid2 != -1) {
      return String.valueOf(uid1) + uid2;
    }
    return String.valueOf(uid1);
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public long getStimulus() {
    throw new UnsupportedOperationException("stimulus");
  }

  @Override
  public void setStimulus(long stimulus) {
    throw new UnsupportedOperationException("stimulus");
  }

  public static class KryoSerializer extends Serializer<HelperProvenanceGraphTuple>
      implements Serializable {

    private final CustomGenericSerializer customGenericSerializer;

    public KryoSerializer(CustomGenericSerializer customGenericSerializer) {
      this.customGenericSerializer = customGenericSerializer;
    }

    public KryoSerializer() {
      this(new CustomGenericSerializer());
    }

    @Override
    public void write(Kryo kryo, Output output, HelperProvenanceGraphTuple object) {
      output.writeLong(object.timestamp);
      output.writeLong(object.rightBoundary);
      kryo.writeObject(output, object.type);
      output.writeBoolean(object.valid);
      output.writeLong(object.count);
      output.writeLong(object.uid1);
      switch (object.type) {
        case SOURCE:
        case SINK:
          customGenericSerializer.write(kryo, output, object.provenanceTuple);
          break;
        case EDGE:
          output.writeLong(object.uid2);
          break;
        case ACK:
          break;
        default:
          throw new IllegalStateException(
              String.format("Unknown ProvenanceGraphTuple type: %s", object.type));
      }
    }

    @Override
    public HelperProvenanceGraphTuple read(Kryo kryo, Input input,
        Class<HelperProvenanceGraphTuple> type) {
      HelperProvenanceGraphTuple object = new HelperProvenanceGraphTuple();
      object.timestamp = input.readLong();
      object.rightBoundary = input.readLong();
      object.type = kryo.readObject(input, ProvenanceGraphNodeType.class);
      object.valid = input.readBoolean();
      object.count = input.readLong();
      object.uid1 = input.readLong();
      object.uid2 = -1;
      switch (object.type) {
        case SOURCE:
        case SINK:
          object.provenanceTuple = (TimestampedUIDTuple) customGenericSerializer.read(kryo, input);
          break;
        case EDGE:
          object.uid2 = input.readLong();
          break;
        case ACK:
          break;
        default:
          throw new IllegalStateException(
              String.format("Unknown ProvenanceGraphTuple type: %s", object.type));
      }
      return object;
    }
  }
}
