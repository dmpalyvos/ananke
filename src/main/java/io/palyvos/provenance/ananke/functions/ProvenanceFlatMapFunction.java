package io.palyvos.provenance.ananke.functions;

import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class ProvenanceFlatMapFunction<T, O>
    implements FlatMapFunction<ProvenanceTupleContainer<T>, ProvenanceTupleContainer<O>> {

  private final FlatMapFunction<T, O> delegate;

  private static class CollectorAdapter<T, O> implements Collector<O> {

    private final ProvenanceTupleContainer<T> input;
    private final Collector<ProvenanceTupleContainer<O>> delegate;

    public CollectorAdapter(
        ProvenanceTupleContainer<T> input, Collector<ProvenanceTupleContainer<O>> delegate) {
      this.input = input;
      this.delegate = delegate;
    }

    @Override
    public void collect(O record) {
      ProvenanceTupleContainer<O> genealogResult = new ProvenanceTupleContainer<>(record);
      GenealogMapHelper.INSTANCE.annotateResult(input, genealogResult);
      genealogResult.copyTimes(input);
      delegate.collect(genealogResult);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  public ProvenanceFlatMapFunction(FlatMapFunction<T, O> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void flatMap(ProvenanceTupleContainer<T> value, Collector<ProvenanceTupleContainer<O>> out)
      throws Exception {
    delegate.flatMap(value.tuple(), new CollectorAdapter<>(value, out));
  }
}
