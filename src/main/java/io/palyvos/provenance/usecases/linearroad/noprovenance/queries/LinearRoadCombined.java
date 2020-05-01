package io.palyvos.provenance.usecases.linearroad.noprovenance.queries;

import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.ACCIDENT_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.ACCIDENT_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.STOPPED_VEHICLE_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.STOPPED_VEHICLE_WINDOW_SLIDE;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadAccidentAggregate;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadSource;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadVehicleAggregate;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.LatencyLoggingSink;
import io.palyvos.provenance.util.TimestampConverter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class LinearRoadCombined {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final TimestampConverter timestampConverter = (ts) -> Time.seconds(ts).toMilliseconds();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();

    FlinkSerializerActivator.NOPROVENANCE.activate(env, settings);

    SingleOutputStreamOperator<LinearRoadInputTuple> sourceStream =
        env.addSource(new LinearRoadSource(settings))
            .setParallelism(1)
            .name("SOURCE")
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<LinearRoadInputTuple>() {
                  @Override
                  public long extractAscendingTimestamp(LinearRoadInputTuple tuple) {
                    return timestampConverter.apply(tuple.getTimestamp());
                  }
                })
            .setParallelism(1);

    sourceStream
        .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
        .setParallelism(env.getParallelism())
        .keyBy(t -> t.getKey())
        .window(
            SlidingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE, STOPPED_VEHICLE_WINDOW_SLIDE))
        .aggregate(new LinearRoadVehicleAggregate())
        .filter(t -> t.getReports() == 4 && t.isUniquePosition())
        .keyBy(t -> t.getLatestPos())
        .window(SlidingEventTimeWindows.of(ACCIDENT_WINDOW_SIZE, ACCIDENT_WINDOW_SLIDE))
        .aggregate(new LinearRoadAccidentAggregate())
        .slotSharingGroup(settings.secondSlotSharingGroup())
        .filter(t -> t.getCount() > 1)
        .addSink(LatencyLoggingSink.newInstance("SINK-ACCIDENT", settings))
        .name("SINK-ACCIDENT")
        .setParallelism(settings.sinkParallelism());

    sourceStream
        .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
        .setParallelism(env.getParallelism())
        .keyBy(t -> t.getKey())
        .window(
            SlidingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE, STOPPED_VEHICLE_WINDOW_SLIDE))
        .aggregate(new LinearRoadVehicleAggregate())
        .slotSharingGroup(settings.secondSlotSharingGroup())
        .filter(v -> v.getReports() == 4 && v.isUniquePosition())
        .addSink(LatencyLoggingSink.newInstance("SINK-STOPPED", settings))
        .name("SINK-STOPPED")
        .setParallelism(settings.sinkParallelism());

    env.execute("LinearRoadCombined");
  }
}
