package io.palyvos.provenance.ananke.functions;

import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;

public class ProvenanceMapFunction<T, O>
    implements MapFunction<ProvenanceTupleContainer<T>, ProvenanceTupleContainer<O>> {

  private final MapFunction<T, O> delegate;

  public ProvenanceMapFunction(MapFunction<T, O> delegate) {
    this.delegate = delegate;
  }

  @Override
  public ProvenanceTupleContainer<O> map(ProvenanceTupleContainer<T> value) throws Exception {
    O result = delegate.map(value.tuple());
    ProvenanceTupleContainer<O> genealogResult = new ProvenanceTupleContainer<>(result);
    GenealogMapHelper.INSTANCE.annotateResult(value, genealogResult);
    genealogResult.copyTimes(value);
    return genealogResult;
  }
}
