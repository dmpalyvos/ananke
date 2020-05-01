package io.palyvos.provenance.usecases.linearroad.provenance;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.usecases.linearroad.noprovenance.VehicleTuple;

public class VehicleTupleGL extends
    VehicleTuple implements GenealogTuple {

  private GenealogData gdata = new GenealogData();

  public VehicleTupleGL(long time, int vid, int reports, int xway, int lane, int dir, int seg,
      int pos, boolean uniquePosition, long stimulus) {
    super(time, vid, reports, xway, lane, dir, seg, pos, uniquePosition, stimulus);
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


  @Override
  public String toString() {
    return super.toString();
  }
}
