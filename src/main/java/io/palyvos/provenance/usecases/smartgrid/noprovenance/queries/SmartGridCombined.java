package io.palyvos.provenance.usecases.smartgrid.noprovenance.queries;

import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_AGGR_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_AGGR_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_JOIN_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_LIMIT;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_AGGREGATE_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_AGGREGATE_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_MAX_COUNT;

import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.CountAggregateFunction;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.MeterUsageAggregateFunction;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.SmartGridSource;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.SmartGridTuple;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.LatencyLoggingSink;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SmartGridCombined {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();

    FlinkSerializerActivator.NOPROVENANCE.activate(env, settings)
        .register(SmartGridTuple.class, new SmartGridTuple.KryoSerializer())
        .register(CountTuple.class, new CountTuple.KryoSerializer());

    DataStream<SmartGridTuple> sourceStream =
        env.addSource(new SmartGridSource(settings))
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<SmartGridTuple>() {
                  @Override
                  public long extractAscendingTimestamp(SmartGridTuple element) {
                    return Time.hours(element.getTimestamp()).toMilliseconds();
                  }
                })
            .setParallelism(1);

    DataStream<SmartGridTuple> midnightStream =
        sourceStream.filter(t -> ((t.getTimestamp() + 1) % 24 == 0))
            .setParallelism(env.getParallelism());

    sourceStream
        .keyBy(s -> s.getKey())
        .window(SlidingEventTimeWindows.of(ANOMALY_AGGR_WINDOW_SIZE, ANOMALY_AGGR_WINDOW_SLIDE))
        .aggregate(new MeterUsageAggregateFunction())
        .setParallelism(env.getParallelism())
        .slotSharingGroup(settings.secondSlotSharingGroup())
        .filter(s -> s.getValue() > ANOMALY_LIMIT)
        .join(midnightStream)
        .where(s -> s.getKey())
        .equalTo(s -> s.getKey())
        .window(TumblingEventTimeWindows.of(ANOMALY_JOIN_WINDOW_SIZE))
        .apply(new SmartGridAnomaly.AnomalyJoinFunction())
        .filter(s -> s.getValue() > ANOMALY_LIMIT)
        .addSink(LatencyLoggingSink.newInstance("SINK-ANOMALY", settings))
        .setParallelism(settings.sinkParallelism());

    sourceStream
        .keyBy(s -> s.getKey())
        .window(
            SlidingEventTimeWindows.of(
                BLACKOUT_AGGREGATE_WINDOW_SIZE, BLACKOUT_AGGREGATE_WINDOW_SLIDE))
        .aggregate(new MeterUsageAggregateFunction())
        .setParallelism(env.getParallelism())
        .filter(s -> s.getValue() == 0)
        .windowAll(
            SlidingEventTimeWindows.of(
                BLACKOUT_AGGREGATE_WINDOW_SIZE, BLACKOUT_AGGREGATE_WINDOW_SLIDE))
        .aggregate(new CountAggregateFunction())
        .slotSharingGroup(settings.secondSlotSharingGroup())
        .filter(c -> c.getCount() > BLACKOUT_MAX_COUNT)
        .addSink(LatencyLoggingSink.newInstance("SINK-BLACKOUT", settings))
        .setParallelism(settings.sinkParallelism());

    env.execute("SmartGridCombined");
  }

  public static class AnomalyJoinFunction
      implements JoinFunction<SmartGridTuple, SmartGridTuple, SmartGridTuple> {

    @Override
    public SmartGridTuple join(SmartGridTuple first, SmartGridTuple second) throws Exception {
      SmartGridTuple result =
          new SmartGridTuple(
              first.getKey(),
              Math.max(first.getTimestamp(), second.getTimestamp()),
              Math.max(first.getStimulus(), second.getStimulus()),
              Math.abs(first.getValue() - second.getValue()));
      return result;
    }
  }
}
