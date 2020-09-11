package io.palyvos.provenance.usecases.smartgrid.provenance.queries;

import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_AGGR_STREAM_KEY;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_AGGR_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_AGGR_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_JOIN_STREAM_KEY;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_JOIN_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_LIMIT;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.usecases.smartgrid.AnomalyOutputSelector;
import io.palyvos.provenance.usecases.smartgrid.provenance.MeterUsageAggregateFunction;
import io.palyvos.provenance.usecases.smartgrid.provenance.SmartGridSourceGL;
import io.palyvos.provenance.usecases.smartgrid.provenance.SmartGridTupleGL;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogDataSerializer;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import io.palyvos.provenance.genealog.GenealogFileSink;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SmartGridAnomaly {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();

    final GenealogDataSerializer genealogDataSerializer =
        GenealogDataSerializer.newInstance(
            settings.aggregateStrategySupplier().get(),
            settings.statisticsFolder(),
            settings.graphTraversalStatistics());

    env.addDefaultKryoSerializer(GenealogData.class, genealogDataSerializer);

    env.addDefaultKryoSerializer(
        SmartGridTupleGL.class, new SmartGridTupleGL.KryoSerializer(genealogDataSerializer));

    SplitStream<SmartGridTupleGL> splitStream =
        env.addSource(new SmartGridSourceGL(settings))
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<SmartGridTupleGL>() {
                  @Override
                  public long extractAscendingTimestamp(SmartGridTupleGL element) {
                    return Time.hours(element.getTimestamp()).toMilliseconds();
                  }
                })
            .split(new AnomalyOutputSelector<>());

    DataStream<SmartGridTupleGL> nonAggregatedStream = splitStream.select(ANOMALY_JOIN_STREAM_KEY);

    splitStream
        .select(ANOMALY_AGGR_STREAM_KEY)
        .keyBy(s -> s.getKey())
        .window(SlidingEventTimeWindows.of(ANOMALY_AGGR_WINDOW_SIZE, ANOMALY_AGGR_WINDOW_SLIDE))
        .aggregate(new MeterUsageAggregateFunction(settings.aggregateStrategySupplier()))
        .slotSharingGroup(settings.secondSlotSharingGroup())
        .filter(s -> s.getValue() > ANOMALY_LIMIT)
        .join(nonAggregatedStream)
        .where(s -> s.getKey())
        .equalTo(s -> s.getKey())
        .window(TumblingEventTimeWindows.of(ANOMALY_JOIN_WINDOW_SIZE))
        .apply(new AnomalyJoinFunctionGL())
        .filter(s -> s.getValue() > ANOMALY_LIMIT)
        .addSink(GenealogFileSink.newInstance(settings))
        .setParallelism(settings.sinkParallelism());

    env.execute("SmartGridAnomaly");
  }

  public static class AnomalyJoinFunctionGL
      implements JoinFunction<SmartGridTupleGL, SmartGridTupleGL, SmartGridTupleGL> {

    @Override
    public SmartGridTupleGL join(SmartGridTupleGL first, SmartGridTupleGL second) throws Exception {
      SmartGridTupleGL result =
          new SmartGridTupleGL(
              first.getKey(),
              Math.max(first.getTimestamp(), second.getTimestamp()),
              Math.max(first.getStimulus(), second.getStimulus()),
              Math.abs(first.getValue() - second.getValue()));
      GenealogJoinHelper.INSTANCE.annotateResult(first, second, result);
      return result;
    }
  }
}
