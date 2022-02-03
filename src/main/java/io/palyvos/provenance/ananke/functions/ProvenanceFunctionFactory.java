package io.palyvos.provenance.ananke.functions;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.ananke.functions.ProvenanceAggregateFunction.GenealogMetadataAccumulator;
import java.io.Serializable;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class ProvenanceFunctionFactory {

  private final Supplier<ProvenanceAggregateStrategy> aggregateStrategy;

  public ProvenanceFunctionFactory(Supplier<ProvenanceAggregateStrategy> aggregateStrategy) {
    this.aggregateStrategy = aggregateStrategy;
  }

  public <T, F extends Function<T, Long> & Serializable> MapFunction<T, ProvenanceTupleContainer<T>> initMap(
      F timestampFunction, F stimulusFunction) {
    return new ProvenanceInitializer<T>(timestampFunction, stimulusFunction);
  }

  public <T> FilterFunction<ProvenanceTupleContainer<T>> filter(FilterFunction<T> delegate) {
    return new ProvenanceFilterFunction<>(delegate);
  }

  public <T, KEY> KeySelector<ProvenanceTupleContainer<T>, KEY> key(KeySelector<T, KEY> delegate) {
    return new ProvenanceKeySelector<>(delegate);
  }


  public <T, KEY> KeySelector<ProvenanceTupleContainer<T>, KEY> key(KeySelector<T, KEY> delegate,
      Class<KEY> clazz) {
    return new ProvenanceKeySelectorWithTypeInfo<>(delegate, clazz);
  }

  public <IN, ACC, OUT>
  AggregateFunction<
      ProvenanceTupleContainer<IN>, GenealogMetadataAccumulator<ACC>, ProvenanceTupleContainer<OUT>>
  aggregate(AggregateFunction<IN, ACC, OUT> delegate) {
    return new ProvenanceAggregateFunction<>(aggregateStrategy, delegate);
  }

  public <T, O> MapFunction<ProvenanceTupleContainer<T>, ProvenanceTupleContainer<O>> map(
      MapFunction<T, O> delegate) {
    return new ProvenanceMapFunction<>(delegate);
  }

  public <T, O> FlatMapFunction<ProvenanceTupleContainer<T>, ProvenanceTupleContainer<O>> flatMap(
      FlatMapFunction<T, O> delegate) {
    return new ProvenanceFlatMapFunction<>(delegate);
  }


  public <IN1, IN2, OUT>
  JoinFunction<
      ProvenanceTupleContainer<IN1>, ProvenanceTupleContainer<IN2>, ProvenanceTupleContainer<OUT>>
  join(JoinFunction<IN1, IN2, OUT> delegate) {
    return new ProvenanceJoinFunction<>(delegate);
  }

  public <IN1, IN2, OUT>
  ProcessJoinFunction<
      ProvenanceTupleContainer<IN1>, ProvenanceTupleContainer<IN2>, ProvenanceTupleContainer<OUT>>
  processJoin(ProcessJoinFunction<IN1, IN2, OUT> delegate) {
    return new ProvenanceProcessJoinFunction<>(delegate);
  }

  public <T> SinkFunction<ProvenanceTupleContainer<T>> sink(SinkFunction<T> delegate) {
    return new ProvenanceSinkFunction<>(delegate);
  }
}
