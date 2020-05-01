package io.palyvos.provenance.ananke.util;

import io.palyvos.provenance.util.TimestampConverter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.watermark.Watermark;

public class WatermarkUpdatingMap<T> extends StreamMap<T, T> {

  private final long maxDelay;
  private final TimestampConverter timestampConverter;

  private WatermarkUpdatingMap(
      MapFunction<T, T> mapFunction, long watermarkShift, TimestampConverter timestampConverter) {
    super(mapFunction);
    this.maxDelay = watermarkShift;
    this.timestampConverter = timestampConverter;
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    long shiftedWatermarkTs = Math.max(0, mark.getTimestamp() - timestampConverter.apply(maxDelay));
    super.processWatermark(new Watermark(shiftedWatermarkTs));
  }

  public static <T> SingleOutputStreamOperator<T> add(
      DataStream<T> stream,
      long watermarkShift,
      TimestampConverter timestampConverter, Class<T> clazz) {

    final MapFunction<T, T> identityFunction = t -> t;

    return stream.transform(
        "Map",
        TypeInformation.of(clazz),
        new WatermarkUpdatingMap<>(identityFunction, watermarkShift, timestampConverter));
  }
}
