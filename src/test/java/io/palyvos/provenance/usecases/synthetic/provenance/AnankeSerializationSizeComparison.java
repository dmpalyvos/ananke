package io.palyvos.provenance.usecases.synthetic.provenance;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogDataSerializer;
import io.palyvos.provenance.ananke.aggregate.SortedPointersAggregateStrategy;
import io.palyvos.provenance.ananke.stdops.HelperProvenanceGraphTuple;
import io.palyvos.provenance.ananke.functions.CustomGenericSerializer;
import java.util.Collections;

public class AnankeSerializationSizeComparison {

  public static void main(String[] args) {
    ExperimentSettings settings = new ExperimentSettings();
    Kryo kryo = new Kryo();
    kryo.setReferences(true);
    final GenealogDataSerializer genealogDataSerializer =
        GenealogDataSerializer.newInstance(new SortedPointersAggregateStrategy(), "", false);

    kryo.register(GenealogData.class, genealogDataSerializer);

    kryo.register(SyntheticSourceTuple.class, new SyntheticSourceTuple.KryoSerializer());

    final CustomGenericSerializer customGenericSerializer = new CustomGenericSerializer();
    customGenericSerializer.register(SyntheticSourceTuple.class,
        new SyntheticSourceTuple.KryoSerializer());

    kryo.register(HelperProvenanceGraphTuple.class,
        new HelperProvenanceGraphTuple.KryoSerializer(customGenericSerializer));

    SyntheticProvenanceSource<SyntheticSinkTuple> syntheticProvenanceSource =
        new SyntheticProvenanceSource<>(SyntheticSinkTuple.supplier(),
            SyntheticSourceTuple.supplier(settings.syntheticTupleSize()), settings);
    syntheticProvenanceSource.initData(0, 1);
    SyntheticSinkTuple sinkTuple =
        syntheticProvenanceSource.newSinkTuple(0, Collections.emptyList());
    TimestampedUIDTuple sourceTuple = sinkTuple.getProvenance().iterator().next();
    System.out.format("Source Tuple Payload = %d bytes\n", settings.syntheticTupleSize());
    System.out.format("# source tuples per sink tuple = %d\n", settings.syntheticProvenanceSize());
    printSerializedSize(kryo, sourceTuple, SyntheticSourceTuple.class, "");
    printSerializedSize(kryo, sinkTuple, SyntheticSinkTuple.class, "");
    sinkTuple.stopSerializingProvenance(); // Done to remove provenance graph from serialization
    printSerializedSize(kryo, HelperProvenanceGraphTuple.newSinkVertex(0, sinkTuple, 0L),
        HelperProvenanceGraphTuple.class, "SINK");
    printSerializedSize(kryo, HelperProvenanceGraphTuple.newSourceVertex(0, sourceTuple, 0L),
        HelperProvenanceGraphTuple.class, "SOURCE");
    printSerializedSize(kryo, HelperProvenanceGraphTuple.newEdge(0, 0L, 0L),
        HelperProvenanceGraphTuple.class, "EDGE");
    printSerializedSize(kryo, HelperProvenanceGraphTuple.newACK(0, 0L),
        HelperProvenanceGraphTuple.class, "ACK");
  }

  private static void printSerializedSize(Kryo kryo, Object object, Class<?> clazz, String info) {
    Output output = new Output(0, -1);
    kryo.writeObject(output, object);
    System.out.format("%s %s = %d bytes%n", clazz.getSimpleName(), info, output.toBytes().length);
    Input input = new Input(output.getBuffer(), 0, output.position());
    kryo.readObject(input, clazz);
  }

}
