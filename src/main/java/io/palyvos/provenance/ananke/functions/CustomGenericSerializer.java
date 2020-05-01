package io.palyvos.provenance.ananke.functions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;

public class CustomGenericSerializer implements Serializable {
  public static class MetaSerializer implements Serializable {
    public final Serializer serializer;
    public final int code;
    public final Class clazz;

    public MetaSerializer(Serializer serializer, int code, Class clazz) {
      this.serializer = serializer;
      this.code = code;
      this.clazz = clazz;
    }
  }

  public static final byte DEFAULT_SERIALIZATION_CODE = Byte.MIN_VALUE;
  public static final int MIN_CLASS_CODE = DEFAULT_SERIALIZATION_CODE + 1;
  private final Map<Class, MetaSerializer> classIndexedMetaSerializers = new HashMap<>();
  private final Map<Integer, MetaSerializer> codeIndexedMetaSerializers = new HashMap<>();
  private final AtomicInteger codeAssigner = new AtomicInteger(MIN_CLASS_CODE);

  public <T, S extends Serializer<T> & Serializable> void register(Class<T> generic, S serializer) {
    int classCode = codeAssigner.getAndIncrement();
    Validate.isTrue(classCode < Byte.MAX_VALUE);
    final MetaSerializer metaSerializer = new MetaSerializer(serializer, classCode, generic);
    classIndexedMetaSerializers.put(generic, metaSerializer);
    codeIndexedMetaSerializers.put(classCode, metaSerializer);
  }

  public void write(Kryo kryo, Output output, Object object) {
    if (writeGenericObject(kryo, output, object)) {
      return;
    }
    output.writeByte(DEFAULT_SERIALIZATION_CODE);
    kryo.writeClassAndObject(output, object);
  }

  private boolean writeGenericObject(Kryo kryo, Output output, Object tuple) {
    MetaSerializer meta = classIndexedMetaSerializers.get(tuple.getClass());
    if (meta == null) {
      return false;
    }
    output.writeByte(meta.code);
    meta.serializer.write(kryo, output, tuple);
    return true;
  }

  public Object read(Kryo kryo, Input input) {
    int classCode = input.readByte();
    Object object;
    if (classCode == DEFAULT_SERIALIZATION_CODE) {
      object = kryo.readClassAndObject(input);
    } else {
      final MetaSerializer meta = codeIndexedMetaSerializers.get(classCode);
      object = meta.serializer.read(kryo, input, meta.clazz);
    }
    return object;
  }
}
