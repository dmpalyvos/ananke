package io.palyvos.provenance.ananke.aggregate;

import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * {@link ProvenanceAggregateStrategy} that maintains the provenance in an {@link ArrayList}. It is
 * able to handle out-of-order arrivals inside each window.
 */
public class ListAggregateStrategy implements ProvenanceAggregateStrategy {

  private final List<GenealogTuple> provenance = new ArrayList<>();

  @Override
  public <T extends GenealogTuple> void addWindowProvenance(T in) {
    provenance.add(in);
  }

  @Override
  public <T extends GenealogTuple> void annotateWindowResult(T result) {
    result.initGenealog(GenealogTupleType.AGGREGATE);
    result.getGenealogData().setAggregateListProvenance(provenance);
  }

  @Override
  public <T extends GenealogTuple> Iterator<GenealogTuple> provenanceIterator(T tuple) {
    return tuple.getGenealogData().getAggregateListProvenance().iterator();
  }
}
