import io.palyvos.provenance.usecases.smartgrid.provenance.SmartGridTupleGL;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.SmartGridTuple;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogDataSerializer;

public class SmartGridSerializationSize {

  private static final String reading = "1,0,0";

  public static void main(String[] args) {
    KryoTestUtil util = new KryoTestUtil();
    util.kryo().register(GenealogData.class, new GenealogDataSerializer());
    util.checkSerializationRound(sgTuple(), SmartGridTuple.class);
    System.out.println(util.getSerializedSizeBytes(sgTuple()));
    util.checkSerializationRound(sgTupleGL(), SmartGridTupleGL.class);
    System.out.println(util.getSerializedSizeBytes(sgTupleGL()));
  }

  public static SmartGridTuple sgTuple() {
    return SmartGridTuple.fromReading(reading);
  }

  public static SmartGridTupleGL sgTupleGL() {
    return SmartGridTupleGL.fromReading(reading);
  }

}
