import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class SmartSerializationTest {

  static final boolean provenance = true;

  public static void main(String[] args) {
    Kryo kryo = new Kryo();
    kryo.setReferences(true);
    kryo.register(GenealogData.class, new SmartSerializer());

    Output output = new Output(1024, -1);
    kryo.writeObject(output, getTupleChain(10));
    System.out.format("BaseTuple = %d bytes%n", output.toBytes().length);
    Input input = new Input(output.getBuffer(), 0, output.position());
    TestTuple object1 = kryo.readObject(input, TestTuple.class);
    System.out.println(object1);
  }

  public static TestTuple getTupleChain(int length) {
    TestTuple t = new TestTuple();
    t.initGenealog(GenealogTupleType.MAP);
    TestTuple prev = t;
    for (int i = 1; i <= length; i++) {
      TestTuple u1 = new TestTuple();
      u1.initGenealog(GenealogTupleType.SOURCE);
      prev.setU1(u1);
      prev = u1;
    }
    return t;
  }

  public static class SmartSerializer extends Serializer<GenealogData> {

    @Override
    public void write(Kryo kryo, Output output, GenealogData object) {
      // TODO: Write null if type == source
      GenealogData root = object;
      GenealogTuple tuple = null;
      while (root.getU1() != null) {
        tuple = root.getU1();
        root = root.getU1().getGenealogData();
      }
      System.out.println("Serializing " + object);
      if (object.getTupleType() == GenealogTupleType.SOURCE) {
        System.out.println("WRITING FALSE");
        output.writeBoolean(false);
      } else {
        System.out.println("WRITING DATA");
        output.writeBoolean(true);
        kryo.writeClassAndObject(output, object.getTupleType());
        kryo.writeClassAndObject(output, tuple);
        kryo.writeClassAndObject(output, object.getUID());
      }
    }

    @Override
    public GenealogData read(Kryo kryo, Input input, Class<GenealogData> type) {
      boolean ser = input.readBoolean();
      if (ser) {
        System.out.println("READING DATA");
        GenealogData gdata = new GenealogData();
        gdata.init((GenealogTupleType) kryo.readClassAndObject(input));
        gdata.setU1((GenealogTuple) kryo.readClassAndObject(input));
        return gdata;
      } else {
        System.out.println("READING NULL");
        return null;
      }
    }
  }

  public static class TestTuple implements GenealogTuple {

    private GenealogData gdata;
    private long a = System.nanoTime();
    private String bla = "1244141";

    @Override
    public GenealogTuple getU1() {
      return gdata.getU1();
    }

    @Override
    public void setU1(GenealogTuple u1) {
      gdata.setU1(u1);
    }

    @Override
    public GenealogTuple getU2() {
      return gdata.getU2();
    }

    @Override
    public void setU2(GenealogTuple u2) {
      gdata.setU2(u2);
    }

    @Override
    public GenealogTuple getNext() {
      return gdata.getNext();
    }

    @Override
    public void setNext(GenealogTuple next) {
      gdata.setNext(next);
    }

    @Override
    public GenealogTupleType getTupleType() {
      return gdata.getTupleType();
    }

    @Override
    public void initGenealog(GenealogTupleType tupleType) {
      gdata = new GenealogData();
      gdata.init(tupleType);
    }

    @Override
    public long getUID() {
      return gdata.getUID();
    }

    @Override
    public void setUID(long uid) {
      gdata.setUID(uid);
    }

    @Override
    public long getStimulus() {
      return 0;
    }

    @Override
    public void setStimulus(long stimulus) {}

    @Override
    public long getTimestamp() {
      return 0;
    }

    @Override
    public void setTimestamp(long timestamp) {}

    @Override
    public String toString() {
      return "Tuple:" + (gdata != null ? gdata.toString() : "");
    }

    @Override
    public GenealogData getGenealogData() {
      return gdata;
    }

  }
}
