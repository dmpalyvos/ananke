package io.palyvos.provenance.usecases.linearroad.provenance.queries;

import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.ACCIDENT_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.ACCIDENT_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.STOPPED_VEHICLE_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.STOPPED_VEHICLE_WINDOW_SLIDE;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadAccidentAggregate;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadInputTupleGL;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadSourceGL;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadVehicleAggregate;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.ProvenanceActivator;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.util.TimestampConverter;
import java.util.Arrays;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class LinearRoadCombined {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final TimestampConverter timestampConverter = (ts) -> Time.seconds(ts).toMilliseconds();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setMaxParallelism(settings.maxParallelism());

    FlinkSerializerActivator.PROVENANCE_OPTIMIZED.activate(env, settings);

    SingleOutputStreamOperator<LinearRoadInputTupleGL> sourceStream =
        env.addSource(new LinearRoadSourceGL(settings))
            .name("SOURCE")
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<LinearRoadInputTupleGL>() {
                  @Override
                  public long extractAscendingTimestamp(LinearRoadInputTupleGL tuple) {
                    return timestampConverter.apply(tuple.getTimestamp());
                  }
                })
            .setParallelism(1)
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .returns(LinearRoadInputTupleGL.class)
            .setParallelism(env.getParallelism());

    SingleOutputStreamOperator<? extends GenealogTuple> accidentStream =
        sourceStream
            .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
            .keyBy(t -> t.getKey())
            .window(
                SlidingEventTimeWindows.of(
                    STOPPED_VEHICLE_WINDOW_SIZE, STOPPED_VEHICLE_WINDOW_SLIDE))
            .aggregate(new LinearRoadVehicleAggregate(settings.aggregateStrategySupplier()))
            .filter(t -> t.getReports() == 4 && t.isUniquePosition())
            .keyBy(t -> t.getLatestPos())
            .window(SlidingEventTimeWindows.of(ACCIDENT_WINDOW_SIZE, ACCIDENT_WINDOW_SLIDE))
            .aggregate(new LinearRoadAccidentAggregate(settings.aggregateStrategySupplier()))
            .slotSharingGroup(settings.secondSlotSharingGroup())
            .filter(t -> t.getCount() > 1);

    SingleOutputStreamOperator<? extends GenealogTuple> stoppedVehiclesStream =
        sourceStream
            .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
            .name("STOPPED-FILTER")
            .keyBy(t -> t.getKey())
            .window(
                SlidingEventTimeWindows.of(
                    STOPPED_VEHICLE_WINDOW_SIZE, STOPPED_VEHICLE_WINDOW_SLIDE))
            .aggregate(new LinearRoadVehicleAggregate(settings.aggregateStrategySupplier()))
            .slotSharingGroup(settings.secondSlotSharingGroup())
            .filter(v -> v.getReports() == 4 && v.isUniquePosition());

    settings
        .genealogActivator()
        .activate(
            Arrays.asList(
                ProvenanceActivator.convert(accidentStream),
                ProvenanceActivator.convert(stoppedVehiclesStream)),
            Arrays.asList("ACCIDENT", "STOPPED"),
            settings,
            ACCIDENT_WINDOW_SIZE.toMilliseconds() + STOPPED_VEHICLE_WINDOW_SIZE.toMilliseconds(),
            timestampConverter);
    env.execute("LinearRoadCombined");
  }
}
