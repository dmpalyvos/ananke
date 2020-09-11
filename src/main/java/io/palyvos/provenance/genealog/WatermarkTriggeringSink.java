package io.palyvos.provenance.genealog;

import java.io.Serializable;
import java.util.function.Consumer;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;

public class WatermarkTriggeringSink<IN> extends StreamSink<IN> {

  private final Consumer<Watermark> watermarkConsumer;

  public WatermarkTriggeringSink(SinkFunction<IN> sinkFunction,
      Consumer<Watermark> watermarkConsumer) {
    super(sinkFunction);
    Validate.notNull(watermarkConsumer, "watermarkConsumer");
    Validate.isInstanceOf(Serializable.class, watermarkConsumer,
        "WatermarkConsumer must be serializable!");
    this.watermarkConsumer = watermarkConsumer;
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    super.processWatermark(mark);
    watermarkConsumer.accept(mark);
  }

  public static <T> void addSink(DataStream<T> stream, String name, int parallelism,
      boolean disableChaining, SinkFunction<T> sinkFunction,
      Consumer<Watermark> watermarkConsumer) {

    if (sinkFunction instanceof InputTypeConfigurable) {
      ((InputTypeConfigurable) sinkFunction)
          .setInputType(stream.getType(), stream.getExecutionConfig());
    }

    StreamSink<T> sinkOperator = new WatermarkTriggeringSink<>(sinkFunction, watermarkConsumer);
    SinkTransformation<T> transformation = new SinkTransformation<T>(stream.getTransformation(),
        name, sinkOperator, parallelism);
    if (disableChaining) {
      transformation.setChainingStrategy(ChainingStrategy.NEVER);
    }
    stream.getExecutionEnvironment().addOperator(transformation);
  }
}
