package io.palyvos.provenance.genealog;

import io.palyvos.provenance.util.TimestampedUIDTuple;
import io.palyvos.provenance.ananke.functions.ProvenanceTupleContainer;
import java.util.Collection;

/**
 * Interface that needs to be implemented by Genealog-compatible tuples. <br>
 * <b>Important: The {@link #equals(Object)} and {@link #hashCode()} methods need to be delegated
 * to
 * the original tuple, in order to avoid duplicate provenance values.</b>
 */
public interface GenealogTuple extends TimestampedUIDTuple {

  void initGenealog(GenealogTupleType tupleType);

  GenealogData getGenealogData();

  default GenealogTuple getU1() {
    return getGenealogData().getU1();
  }

  default void setU1(GenealogTuple u1) {
    getGenealogData().setU1(u1);
  }

  default GenealogTuple getU2() {
    return getGenealogData().getU2();
  }

  default void setU2(GenealogTuple u2) {
    getGenealogData().setU2(u2);
  }

  default GenealogTuple getNext() {
    return getGenealogData().getNext();
  }

  default void setNext(GenealogTuple next) {
    getGenealogData().setNext(next);
  }

  default boolean trySetNext(GenealogTuple expect, GenealogTuple next) {
    return getGenealogData().trySetNext(expect, next);
  }

  default void stopSerializingProvenance() {
    getGenealogData().setTupleType(GenealogTupleType.META_SINK);
  }

  default GenealogTupleType getTupleType() {
    return getGenealogData().getTupleType();
  }

  default long getUID() {
    return getGenealogData().getUID();
  }

  default void setUID(long uid) {
    getGenealogData().setUID(uid);
  }

  default Collection<TimestampedUIDTuple> getProvenance() {
    return getGenealogData().getProvenance();
  }

  default TimestampedUIDTuple getSourceTuple() {
    throw new UnsupportedOperationException(
        "Only enabled in special subclasses " + ProvenanceTupleContainer.class.getSimpleName());
  }

}
