package io.palyvos.provenance.genealog;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class GenealogDataSerializer extends Serializer<GenealogData> implements Serializable {

  private GenealogGraphTraverser genealogGraphTraverser;
  private static final byte SOURCE_TUPLE_CODE = 1;
  private static final byte REMOTE_TUPLE_CODE = 2;
  private static final byte META_SINK_TUPLE_CODE = 3;
  private static final byte META_SOURCE_TUPLE_CODE = 4;

  public static GenealogDataSerializer newInstance(
      ProvenanceAggregateStrategy provenanceAggregateStrategy,
      String statisticsFolder,
      boolean graphTraversalStatistics) {
    return (graphTraversalStatistics)
        ? new GenealogDataSerializerTraversalStatistics(provenanceAggregateStrategy, statisticsFolder)
        : new GenealogDataSerializer(provenanceAggregateStrategy);
  }

  public GenealogDataSerializer(ProvenanceAggregateStrategy aggregateStrategy) {
    this.genealogGraphTraverser = new GenealogGraphTraverser(aggregateStrategy);
  }

  public GenealogDataSerializer() {
    // For serialization/deserialization purposes
  }

  @Override
  public void write(Kryo kryo, Output output, GenealogData object) {
    output.writeLong(object.getUID());
    switch (object.getTupleType()) {
      case SOURCE:
        output.writeByte(SOURCE_TUPLE_CODE);
        break;
      case META_SINK:
        output.writeByte(META_SINK_TUPLE_CODE);
        break;
      case MAP:
      case AGGREGATE:
      case JOIN:
      case REMOTE:
        output.writeByte(REMOTE_TUPLE_CODE);
        kryo.writeObject(output, getProvenance(object));
        break;
      case META_SOURCE:
        output.writeByte(META_SOURCE_TUPLE_CODE);
        break;
      default:
        throw new IllegalStateException(
            "No rule to serialize tuple with type " + object.getTupleType());
    }
  }

  protected Set<TimestampedUIDTuple> getProvenance(GenealogData object) {
    return genealogGraphTraverser.getProvenance(object);
  }

  @Override
  public GenealogData read(Kryo kryo, Input input, Class<GenealogData> type) {
    final long uid = input.readLong();
    final int tupleCode = input.readByte();
    GenealogTupleType tupleType;
    Collection<TimestampedUIDTuple> provenance = null;
    switch (tupleCode) {
      case SOURCE_TUPLE_CODE:
        tupleType = GenealogTupleType.SOURCE;
        break;
      case META_SINK_TUPLE_CODE:
        tupleType = GenealogTupleType.META_SINK;
        break;
      case REMOTE_TUPLE_CODE:
        tupleType = GenealogTupleType.REMOTE;
        provenance = (Collection<TimestampedUIDTuple>) kryo.readObject(input, HashSet.class);
        break;
      case META_SOURCE_TUPLE_CODE:
        tupleType = GenealogTupleType.META_SOURCE;
        break;
      default:
        throw new IllegalStateException("Unknown tupleCode: " + tupleCode);
    }
    final GenealogData data = new GenealogData();
    data.init(tupleType);
    data.setUID(uid);
    data.setProvenance(provenance);
    return data;
  }
}
