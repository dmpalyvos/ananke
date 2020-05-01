package io.palyvos.provenance.ananke.functions;

import org.apache.flink.api.java.functions.KeySelector;

public class ProvenanceKeySelector<IN, KEY> implements KeySelector<ProvenanceTupleContainer<IN>, KEY> {

  private final KeySelector<IN, KEY> delegate;

  public ProvenanceKeySelector(KeySelector<IN, KEY> delegate) {
    this.delegate = delegate;
  }

  @Override
  public KEY getKey(ProvenanceTupleContainer<IN> value) throws Exception {
    return delegate.getKey(value.tuple());
  }
}
