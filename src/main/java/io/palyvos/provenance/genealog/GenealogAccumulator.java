package io.palyvos.provenance.genealog;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.util.Accumulator;
import java.io.Serializable;
import java.util.function.Supplier;

public abstract class GenealogAccumulator<
        IN extends GenealogTuple, OUT extends GenealogTuple, T extends GenealogAccumulator>
    implements Serializable, Accumulator<IN, OUT, T> {

  private final ProvenanceAggregateStrategy strategy;

  public GenealogAccumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
    strategy = strategySupplier.get();
  }

  @Override
  public final void add(IN tuple) {
    strategy.addWindowProvenance(tuple);
    doAdd(tuple);
  }

  protected abstract void doAdd(IN tuple);

  @Override
  public final OUT getAggregatedResult() {
    OUT result = doGetAggregatedResult();
    strategy.annotateWindowResult(result);
    return result;
  }

  protected abstract OUT doGetAggregatedResult();

  public final T merge(T other) {
    throw new UnsupportedOperationException("merge");
  }

  protected void doMerge(T other) {
    throw new UnsupportedOperationException("merge");
  };
}
