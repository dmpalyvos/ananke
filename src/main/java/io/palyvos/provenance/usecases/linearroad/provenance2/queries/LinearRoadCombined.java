package io.palyvos.provenance.usecases.linearroad.provenance2.queries;

import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.ACCIDENT_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.ACCIDENT_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.STOPPED_VEHICLE_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.STOPPED_VEHICLE_WINDOW_SLIDE;

import io.palyvos.provenance.util.ProvenanceActivator;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadAccidentAggregate;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadSource;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadVehicleAggregate;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.util.TimestampConverter;
import io.palyvos.provenance.ananke.functions.ProvenanceFunctionFactory;
import io.palyvos.provenance.ananke.functions.ProvenanceTupleContainer;
import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
    final ProvenanceFunctionFactory GL =
        new ProvenanceFunctionFactory(settings.aggregateStrategySupplier());

    FlinkSerializerActivator.PROVENANCE_TRANSPARENT.activate(env, settings);

    SingleOutputStreamOperator<ProvenanceTupleContainer<LinearRoadInputTuple>> sourceStream =
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
            .map(
                GL.initMap(
                    (Serializable & Function<LinearRoadInputTuple, Long>) t -> t.getTimestamp(),
                    (Serializable & Function<LinearRoadInputTuple, Long>) t -> t.getStimulus()))
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .returns(new TypeHint<ProvenanceTupleContainer<LinearRoadInputTuple>>() {})
            .setParallelism(env.getParallelism());

    SingleOutputStreamOperator<? extends GenealogTuple> accidentStream =
        sourceStream
            .filter(GL.filter(t -> t.getType() == 0 && t.getSpeed() == 0))
            .keyBy(GL.key(t -> t.getKey()), TypeInformation.of(String.class))
            .window(
                SlidingEventTimeWindows.of(
                    STOPPED_VEHICLE_WINDOW_SIZE, STOPPED_VEHICLE_WINDOW_SLIDE))
            .aggregate(
                GL.aggregate(
                    new LinearRoadVehicleAggregate()))
            .filter(GL.filter(t -> t.getReports() == 4 && t.isUniquePosition()))
            .keyBy(GL.key(t -> t.getLatestPos()), TypeInformation.of(Integer.class))
            .window(SlidingEventTimeWindows.of(ACCIDENT_WINDOW_SIZE, ACCIDENT_WINDOW_SLIDE))
            .aggregate(GL.aggregate(new LinearRoadAccidentAggregate()))
            .slotSharingGroup(settings.secondSlotSharingGroup())
            .filter(GL.filter(t -> t.getCount() > 1));

    SingleOutputStreamOperator<? extends GenealogTuple> stoppedVehiclesStream =
        sourceStream
            .filter(GL.filter(t -> t.getType() == 0 && t.getSpeed() == 0))
            .keyBy(GL.key(t -> t.getKey()), TypeInformation.of(String.class))
            .window(
                SlidingEventTimeWindows.of(
                    STOPPED_VEHICLE_WINDOW_SIZE, STOPPED_VEHICLE_WINDOW_SLIDE))
            .aggregate(GL.aggregate(new LinearRoadVehicleAggregate()))
            .slotSharingGroup(settings.secondSlotSharingGroup())
            .filter(GL.filter(v -> v.getReports() == 4 && v.isUniquePosition()));

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
