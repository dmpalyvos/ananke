package io.palyvos.provenance.ananke.aggregate;

import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import java.util.Iterator;
import org.apache.commons.lang3.Validate;

/**
 * {@link ProvenanceAggregateStrategy} that maintains provenance as a pointer list. The ordering of
 * tuples in the list is arbitrary (depending on tuple arrival order). It does not support
 * out-of-order arrivals of tuples in the window.
 */
public class UnsortedPointersAggregateStrategy implements ProvenanceAggregateStrategy {

  GenealogTuple earliest;
  GenealogTuple latest;

  @Override
  public final <T extends GenealogTuple> void addWindowProvenance(T in) {
    if (earliest == null) {
      earliest = in;
      latest = in;
    } else {
      doInsert(in);
    }
  }

  protected <T extends GenealogTuple> void doInsert(T in) {
    latest.setNext(in);
    latest = in;
  }

  @Override
  public final <T extends GenealogTuple> void annotateWindowResult(T result) {
    result.initGenealog(GenealogTupleType.AGGREGATE);
    // The pointer chain goes from the earliest to the latest tuple
    // (U2 (earliest) -> ... -> U1(latest)
    result.setU1(latest);
    result.setU2(earliest);
  }

  @Override
  public final <T extends GenealogTuple> Iterator<GenealogTuple> provenanceIterator(T tuple) {
    return new PointerListIterator(tuple.getU2(), tuple.getU1());
  }

  @Override
  public ProvenanceAggregateStrategy merge(ProvenanceAggregateStrategy other) {
    Validate.notNull(other, "other");
    Validate.isInstanceOf(UnsortedPointersAggregateStrategy.class, other,
        "Only ProvenanceAggregateStrategies of same type can be merged!");
    UnsortedPointersAggregateStrategy otherPointersStrategy = (UnsortedPointersAggregateStrategy) other;
    latest.setNext(otherPointersStrategy.earliest);
    latest = otherPointersStrategy.latest;
    return this;
  }

}
