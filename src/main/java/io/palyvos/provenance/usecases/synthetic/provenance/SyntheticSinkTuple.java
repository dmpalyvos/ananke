package io.palyvos.provenance.usecases.synthetic.provenance;

import io.palyvos.provenance.util.IncreasingUIDGenerator;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import java.io.Serializable;
import java.util.function.Supplier;

public class SyntheticSinkTuple implements Serializable, GenealogTuple {

  private GenealogData genealogData;
  private long timestamp;
  private long stimulus;

  static class DummySinkTupleSupplier implements Supplier<SyntheticSinkTuple>,
      Serializable {

    @Override
    public SyntheticSinkTuple get() {
      return new SyntheticSinkTuple();
    }
  }

  public static DummySinkTupleSupplier supplier() {
    return new DummySinkTupleSupplier();
  }

  public SyntheticSinkTuple() {
    initGenealog(GenealogTupleType.REMOTE);
  }

  @Override
  public void initGenealog(GenealogTupleType tupleType) {
    genealogData = new GenealogData();
    genealogData.init(tupleType);
  }

  @Override
  public GenealogData getGenealogData() {
    return genealogData;
  }


  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
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
  public String toString() {
    return String.format("K-%d,%s", timestamp, IncreasingUIDGenerator.asString(getUID()));
  }
}
