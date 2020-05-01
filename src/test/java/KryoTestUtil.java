import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import javax.xml.bind.DatatypeConverter;

public class KryoTestUtil {

  private final Kryo kryo;

  public KryoTestUtil() {
    this.kryo = new Kryo();
    kryo.setReferences(true);
  }

  public <T> int getSerializedSizeBytes(T obj) {
    kryo.setReferences(true);
    Output output = new Output(1024, -1);
    kryo.writeObject(output, obj);
    return output.toBytes().length;
  }

  public <T> void checkSerializationRound(T obj, Class<T> clazz) {
    System.out.format("Serializing %s %s%n", clazz.getSimpleName(), obj);
    Output output = new Output(1024, -1);
    kryo.writeObject(output, obj);
    System.out.println(DatatypeConverter.printHexBinary(output.toBytes()));
    Input input = new Input(output.getBuffer(), 0, output.position());
    T deserialized = kryo.readObject(input, clazz);
    System.out.format("Deserialized %s %s%n", clazz.getSimpleName(), deserialized);
  }

  public Kryo kryo() {
    return kryo;
  }
}
