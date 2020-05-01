package io.palyvos.provenance.genealog;



public enum GenealogMapHelper {
  INSTANCE;

  public <T extends GenealogTuple, O extends GenealogTuple> void annotateResult(T in, O result) {
    result.initGenealog(GenealogTupleType.MAP);
    result.setU1(in);
  }

}
