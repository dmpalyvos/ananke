package io.palyvos.provenance.ananke.functions;

import io.palyvos.provenance.genealog.GenealogJoinHelper;
import org.apache.commons.lang3.Validate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class ProvenanceProcessJoinFunction<IN1, IN2, OUT> extends
    ProcessJoinFunction<ProvenanceTupleContainer<IN1>, ProvenanceTupleContainer<IN2>, ProvenanceTupleContainer<OUT>> {

  private final ProcessJoinFunction<IN1, IN2, OUT> delegate;

  protected ProvenanceProcessJoinFunction(
      ProcessJoinFunction<IN1, IN2, OUT> delegate) {
    Validate.notNull(delegate, "delegate");
    this.delegate = delegate;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    delegate.open(parameters);
  }

  @Override
  public void processElement(ProvenanceTupleContainer<IN1> left,
      ProvenanceTupleContainer<IN2> right,
      ProcessJoinFunction<ProvenanceTupleContainer<IN1>, ProvenanceTupleContainer<IN2>, ProvenanceTupleContainer<OUT>>.Context ctx,
      Collector<ProvenanceTupleContainer<OUT>> out) throws Exception {
    delegate.processElement(left.tuple(), right.tuple(),
        (ProcessJoinFunction<IN1, IN2, OUT>.Context) ctx, new CollectorAdapter<>(left, right, out));
  }


  private static class CollectorAdapter<T1, T2, O> implements Collector<O> {

    private final ProvenanceTupleContainer<T1> left;
    private final ProvenanceTupleContainer<T2> right;
    private final Collector<ProvenanceTupleContainer<O>> delegate;

    public CollectorAdapter(ProvenanceTupleContainer<T1> left, ProvenanceTupleContainer<T2> right,
        Collector<ProvenanceTupleContainer<O>> delegate) {
      this.left = left;
      this.right = right;
      this.delegate = delegate;
    }

    @Override
    public void collect(O record) {
      ProvenanceTupleContainer<O> genealogResult = new ProvenanceTupleContainer<>(record);
      genealogResult.copyTimes(left, right);
      GenealogJoinHelper.INSTANCE.annotateResult(left, right, genealogResult);
      delegate.collect(genealogResult);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

}
