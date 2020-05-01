package io.palyvos.provenance.util;

public interface Accumulator<IN, OUT, T> {

  void add(IN tuple);

  OUT getAggregatedResult();

  T merge(T other);
}
