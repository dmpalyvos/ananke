package io.palyvos.provenance.usecases.cars;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import org.apache.flink.api.java.tuple.Tuple4;

public class Tuple4GL<T1, T2, T3, T4> extends Tuple4<T1, T2, T3, T4> implements GenealogTuple {

  //Fields need to be public for Flink to serialize correctly
  public long stimulus;
  public long timestamp;
  public GenealogData genealogData;

  public Tuple4GL() {
  }

  public Tuple4GL(T1 value0, T2 value1, T3 value2, T4 value3) {
    super(value0, value1, value2, value3);
  }

  @Override
  public void initGenealog(GenealogTupleType tupleType) {
    this.genealogData = new GenealogData();
    this.genealogData.init(tupleType);
  }

  @Override
  public GenealogData getGenealogData() {
    return genealogData;
  }

  public void setGenealogData(GenealogData genealogData) {
    this.genealogData = genealogData;
  }

  @Override
  public long getStimulus() {
    return stimulus;
  }

  @Override
  public void setStimulus(long stimulus) {
    this.stimulus = stimulus;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
}
