package io.palyvos.provenance.genealog;


public enum GenealogJoinHelper {
  INSTANCE;

  public <T1 extends GenealogTuple, T2 extends GenealogTuple, O extends GenealogTuple> void annotateResult(
      T1 t1, T2 t2, O result) {
    result.initGenealog(GenealogTupleType.JOIN);
    result.setU1(t1);
    result.setU2(t2);
  }

}
