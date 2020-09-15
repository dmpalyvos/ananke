package io.palyvos.provenance.usecases.linearroad.provenance.queries;

import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.STOPPED_VEHICLE_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.STOPPED_VEHICLE_WINDOW_SLIDE;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadInputTupleGL;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadSourceGL;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadVehicleAggregate;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogDataSerializer;
import io.palyvos.provenance.genealog.GenealogFileSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class LinearRoadStoppedVehicles {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();

    env.addDefaultKryoSerializer(
        GenealogData.class,
        GenealogDataSerializer.newInstance(
            settings.aggregateStrategySupplier().get(),
            settings.statisticsFolder(),
            settings.graphTraversalStatistics()));

    env.addSource(new LinearRoadSourceGL(settings))
        .assignTimestampsAndWatermarks(
            new AscendingTimestampExtractor<LinearRoadInputTupleGL>() {
              @Override
              public long extractAscendingTimestamp(LinearRoadInputTupleGL tuple) {
                return Time.seconds(tuple.getTimestamp()).toMilliseconds();
              }
            })
        .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
        .keyBy(t -> t.getKey())
        .window(
            SlidingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE, STOPPED_VEHICLE_WINDOW_SLIDE))
        .aggregate(new LinearRoadVehicleAggregate(settings.aggregateStrategySupplier()))
        .slotSharingGroup(settings.secondSlotSharingGroup())
        .filter(v -> v.getReports() == 4 && v.isUniquePosition())
        .addSink(GenealogFileSink.newInstance(settings))
        .setParallelism(settings.sinkParallelism());

    env.execute("LinearRoadStoppedVehicles");
  }
}
