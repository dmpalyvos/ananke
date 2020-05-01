package io.palyvos.provenance.util;

public abstract class BaseAccumulator<IN, OUT, T extends BaseAccumulator> implements Accumulator<IN, OUT, T> {

  @Override
  public void add(IN tuple) {
    doAdd(tuple);
  }

  protected abstract void doAdd(IN tuple);

  @Override
  public OUT getAggregatedResult() {
    return doGetAggregatedResult();
  }

  protected abstract OUT doGetAggregatedResult();

  public final T merge(T other) {
    doMerge(other);
    return (T) this;
  }

  protected abstract void doMerge(T other);
}
