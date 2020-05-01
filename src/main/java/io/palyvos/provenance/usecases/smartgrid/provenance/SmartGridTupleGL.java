package io.palyvos.provenance.usecases.smartgrid.provenance;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogDataSerializer;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.SmartGridTuple;
import java.io.Serializable;
import java.util.regex.Pattern;

/** Tuple with schema {@code (id, ts, stimulus, value)} */
public class SmartGridTupleGL extends SmartGridTuple
    implements GenealogTuple {

  private static final Pattern DELIMITER_PATTERN = Pattern.compile(",");
  private GenealogData gdata;

  public SmartGridTupleGL() {}

  protected SmartGridTupleGL(String[] readings) {
    super(readings);
  }

  public SmartGridTupleGL(String key, long timestamp, long stimulus, long value) {
    super(key, timestamp, stimulus, value);
  }

  public static SmartGridTupleGL fromReading(String meterReading) {
    try {
      String[] readings = DELIMITER_PATTERN.split(meterReading.trim());
      SmartGridTupleGL tuple = new SmartGridTupleGL(readings);
      tuple.initGenealog(GenealogTupleType.SOURCE);
      return tuple;
    } catch (Exception exception) {
      throw new IllegalArgumentException(
          String.format("Failed to parse reading: %s", meterReading), exception);
    }
  }

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
  public GenealogData getGenealogData() {
    return gdata;
  }

  public static class KryoSerializer extends com.esotericsoftware.kryo.Serializer<SmartGridTupleGL>
      implements Serializable {

    private GenealogDataSerializer genealogDataSerializer;

    public KryoSerializer(GenealogDataSerializer genealogDataSerializer) {
      this.genealogDataSerializer = genealogDataSerializer;
    }

    @Override
    public void write(Kryo kryo, Output output, SmartGridTupleGL object) {
      output.writeString(object.getKey());
      output.writeLong(object.getTimestamp());
      output.writeLong(object.getStimulus());
      output.writeLong(object.getValue());
      genealogDataSerializer.write(kryo, output, object.gdata);
    }

    @Override
    public SmartGridTupleGL read(Kryo kryo, Input input, Class<SmartGridTupleGL> type) {
      SmartGridTupleGL tuple =
          new SmartGridTupleGL(
              input.readString(), input.readLong(), input.readLong(), input.readLong());
      tuple.gdata = genealogDataSerializer.read(kryo, input, GenealogData.class);
      return tuple;
    }
  }
}
