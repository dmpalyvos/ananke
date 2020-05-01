import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import java.io.Serializable;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Verify that serializing a provenance tuple with a transient null reference to provenance data
 * is exactly the same as not having the provenance data field at all.
 */
public class SerializationSizeComparison {

  final static boolean provenance = true;

  public static void main(String[] args) {
    Kryo kryo = new Kryo();
    kryo.setReferences(true);
    if (provenance) {
      kryo.register(GenealogTuple.class, new ProvenanceSerializer());
    }

    Output output = new Output(1024, -1);
    kryo.writeObject(output, new BaseTupleAAAAAAAAA());
    System.out.format("BaseTuple = %d bytes%n", output.toBytes().length);
    Input input = new Input(output.getBuffer(), 0, output.position());
    BaseTupleAAAAAAAAA object1 = kryo.readObject(input, BaseTupleAAAAAAAAA.class);
    System.out.println(object1);

    Output output2 = new Output(1024, -1);
    kryo.writeObject(output2, new BaseTupleWithExtra().setup());
    System.out.format("BaseTupleExtra = %d bytes%n", output2.toBytes().length);
    Input input2 = new Input(output2.getBuffer(), 0, output2.position());
    BaseTupleWithExtra object2 = kryo.readObject(input2, BaseTupleWithExtra.class);
    System.out.println(object2);
  }

  public static class BaseTupleAAAAAAAAA implements Serializable {

    private long value = System.currentTimeMillis();

    @Override
    public String toString() {
      return new ToStringBuilder(this)
          .append("value", value)
          .appendSuper(super.toString())
          .toString();
    }
  }

  public static class BaseTupleWithExtra implements Serializable {

    private long value = System.currentTimeMillis();
    private transient GenealogData pd = new GenealogData();

    public BaseTupleWithExtra setup() {
      pd.init(GenealogTupleType.SOURCE);
      return this;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this)
          .append("value", value)
          .append("pd", pd)
          .appendSuper(super.toString())
          .toString();
    }
  }

  public static class ProvenanceSerializer extends Serializer<BaseTupleWithExtra> {

    @Override
    public void write(Kryo kryo, Output output, BaseTupleWithExtra object) {
      output.writeLong(object.value);
      kryo.writeClassAndObject(output, object.pd);
    }

    @Override
    public BaseTupleWithExtra read(Kryo kryo, Input input, Class<BaseTupleWithExtra> type) {
      BaseTupleWithExtra e = new BaseTupleWithExtra();
      e.value = input.readLong();
      e.pd = (GenealogData) kryo.readClassAndObject(input);
      return e;
    }
  }

}
