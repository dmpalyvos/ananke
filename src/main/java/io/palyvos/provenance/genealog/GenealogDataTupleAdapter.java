package io.palyvos.provenance.genealog;

public class GenealogDataTupleAdapter implements GenealogTuple {

  public static final String UNSUPPORTED_ERROR =
      "This class is only meant to be a helper to traverse "
          + "the provenance graph from a GenealogData object!";
  private final GenealogData gdata;

  public GenealogDataTupleAdapter(GenealogData gdata) {
    this.gdata = gdata;
  }

  @Override
  public GenealogData getGenealogData() {
    return gdata;
  }

  @Override
  public void initGenealog(GenealogTupleType tupleType) {
    throw new UnsupportedOperationException(UNSUPPORTED_ERROR);
  }

  @Override
  public long getStimulus() {
    throw new UnsupportedOperationException(UNSUPPORTED_ERROR);
  }

  @Override
  public void setStimulus(long stimulus) {
    throw new UnsupportedOperationException(UNSUPPORTED_ERROR);
  }

  @Override
  public long getTimestamp() {
    throw new UnsupportedOperationException(UNSUPPORTED_ERROR);
  }

  @Override
  public void setTimestamp(long timestamp) {
    throw new UnsupportedOperationException(UNSUPPORTED_ERROR);
  }

  @Override
  public String toString() {
    return "GenealogDataTupleAdapter{" + "gdata=" + gdata + '}';
  }
}
