import io.palyvos.provenance.usecases.smartgrid.provenance.SmartGridTupleGL;
import io.palyvos.provenance.usecases.smartgrid.provenance.SmartGridTupleGL.KryoSerializer;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.SmartGridTuple;
import io.palyvos.provenance.genealog.GenealogDataSerializer;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.ananke.aggregate.UnsortedPointersAggregateStrategy;

public class SmartGridSerializationSizeWithProvenance {

  private static final String reading = "1,0,0";

  public static void main(String[] args) {
    KryoTestUtil util = new KryoTestUtil();
    util.kryo().register(SmartGridTupleGL.class, new KryoSerializer(
        new GenealogDataSerializer(new UnsortedPointersAggregateStrategy())));
    util.checkSerializationRound(sgTuple(), SmartGridTuple.class);
    System.out.println(util.getSerializedSizeBytes(sgTuple()));
    util.checkSerializationRound(sgTupleGL(), SmartGridTupleGL.class);
    System.out.println(util.getSerializedSizeBytes(sgTupleGL()));
  }

  public static SmartGridTuple sgTuple() {
    return SmartGridTuple.fromReading(reading);
  }

  public static SmartGridTupleGL sgTupleGL() {
    SmartGridTupleGL source = SmartGridTupleGL.fromReading(reading);
    SmartGridTupleGL second = new SmartGridTupleGL("2", 1, 1, 1);
    second.initGenealog(GenealogTupleType.MAP);
    second.setU1(source);
    return second;
  }

}
