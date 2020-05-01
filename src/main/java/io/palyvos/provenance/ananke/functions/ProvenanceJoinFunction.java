package io.palyvos.provenance.ananke.functions;

import io.palyvos.provenance.genealog.GenealogJoinHelper;
import org.apache.flink.api.common.functions.JoinFunction;

public class ProvenanceJoinFunction<IN1, IN2, OUT>
    implements JoinFunction<
    ProvenanceTupleContainer<IN1>, ProvenanceTupleContainer<IN2>, ProvenanceTupleContainer<OUT>> {

  private final JoinFunction<IN1, IN2, OUT> delegate;

  public ProvenanceJoinFunction(JoinFunction<IN1, IN2, OUT> delegate) {
    this.delegate = delegate;
  }

  @Override
  public ProvenanceTupleContainer<OUT> join(
      ProvenanceTupleContainer<IN1> first, ProvenanceTupleContainer<IN2> second) throws Exception {
    OUT result = delegate.join(first.tuple(), second.tuple());
    ProvenanceTupleContainer<OUT> genealogResult = new ProvenanceTupleContainer<>(result);
    genealogResult.copyTimes(first, second);
    GenealogJoinHelper.INSTANCE.annotateResult(first, second, genealogResult);
    return genealogResult;
  }
}
