package io.palyvos.provenance.usecases.cars;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import org.apache.flink.api.java.tuple.Tuple2;

public class Tuple2GL<T1, T2> extends Tuple2<T1, T2> implements GenealogTuple {

  //Fields need to be public for Flink to serialize correctly
  public long stimulus;
  public long timestamp;
  public GenealogData genealogData;

  public Tuple2GL() {
  }

  public Tuple2GL(T1 value0, T2 value1) {
    super(value0, value1);
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
