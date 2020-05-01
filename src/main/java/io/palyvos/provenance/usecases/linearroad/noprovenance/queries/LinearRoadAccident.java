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
import io.palyvos.provenance.util.LatencyLoggingSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class LinearRoadAccident {


  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();

    env.addSource(new LinearRoadSource(settings))
        .assignTimestampsAndWatermarks(
        new AscendingTimestampExtractor<LinearRoadInputTuple>() {
          @Override
          public long extractAscendingTimestamp(LinearRoadInputTuple tuple) {
            return Time.seconds(tuple.getTimestamp()).toMilliseconds();
          }
        })
        .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
        .keyBy(t -> t.getKey())
        .window(SlidingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE,
            STOPPED_VEHICLE_WINDOW_SLIDE))
        .aggregate(new LinearRoadVehicleAggregate())
        .filter(t -> t.getReports() == 4 && t.isUniquePosition())
        .keyBy(t -> t.getLatestPos())
        .window(SlidingEventTimeWindows.of(ACCIDENT_WINDOW_SIZE,
            ACCIDENT_WINDOW_SLIDE))
        .aggregate(new LinearRoadAccidentAggregate())
        .slotSharingGroup(settings.secondSlotSharingGroup())
        .filter(t -> t.getCount() > 1)
        .addSink(LatencyLoggingSink.newInstance(settings))
        .setParallelism(settings.sinkParallelism());

    env.execute("LinearRoadAccident");

  }
}
