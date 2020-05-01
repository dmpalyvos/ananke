package io.palyvos.provenance.usecases.smartgrid.provenance2.queries;

import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_AGGR_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_AGGR_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_JOIN_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_LIMIT;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_AGGREGATE_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_AGGREGATE_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_MAX_COUNT;

import io.palyvos.provenance.util.ProvenanceActivator;
import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.CountAggregateFunction;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.MeterUsageAggregateFunction;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.SmartGridSource;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.SmartGridTuple;
import io.palyvos.provenance.util.BaseTuple;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.util.TimestampConverter;
import io.palyvos.provenance.ananke.functions.ProvenanceFunctionFactory;
import io.palyvos.provenance.ananke.functions.ProvenanceTupleContainer;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.queries.SmartGridAnomaly.AnomalyJoinFunction;
import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
    final TimestampConverter timestampConverter = (ts) -> Time.hours(ts).toMilliseconds();
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setMaxParallelism(settings.maxParallelism());
    final ProvenanceFunctionFactory GL =
        new ProvenanceFunctionFactory(settings.aggregateStrategySupplier());

    FlinkSerializerActivator.PROVENANCE_TRANSPARENT.activate(env, settings)
        .register(SmartGridTuple.class, new SmartGridTuple.KryoSerializer())
        .register(CountTuple.class, new CountTuple.KryoSerializer());

    DataStream<ProvenanceTupleContainer<SmartGridTuple>> sourceStream =
        env.addSource(new SmartGridSource(settings))
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<SmartGridTuple>() {
                  @Override
                  public long extractAscendingTimestamp(SmartGridTuple element) {
                    return timestampConverter.apply(element.getTimestamp());
                  }
                })
            .setParallelism(1)
            .map(
                GL.initMap(
                    (Serializable & Function<SmartGridTuple, Long>) t -> t.getTimestamp(),
                    (Serializable & Function<SmartGridTuple, Long>) t -> t.getStimulus()))
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .returns(new TypeHint<ProvenanceTupleContainer<SmartGridTuple>>() {
            })
            .setParallelism(env.getParallelism());

    DataStream<ProvenanceTupleContainer<SmartGridTuple>> midnightStream =
        sourceStream.filter(GL.filter(t -> ((t.getTimestamp() + 1) % 24 == 0)));

    DataStream<? extends GenealogTuple> anomalyStream =
        sourceStream
            .keyBy(GL.key(s -> s.getKey()), TypeInformation.of(String.class))
            .window(SlidingEventTimeWindows.of(ANOMALY_AGGR_WINDOW_SIZE, ANOMALY_AGGR_WINDOW_SLIDE))
            .aggregate(GL.aggregate(new MeterUsageAggregateFunction()))
            .slotSharingGroup(settings.secondSlotSharingGroup())
            .filter(GL.filter(s -> s.getValue() > ANOMALY_LIMIT))
            .join(midnightStream)
            .where(GL.key(BaseTuple::getKey, String.class))
            .equalTo(GL.key(BaseTuple::getKey, String.class))
            .window(TumblingEventTimeWindows.of(ANOMALY_JOIN_WINDOW_SIZE))
            .apply(GL.join(new AnomalyJoinFunction()))
            .filter(GL.filter(s -> s.getValue() > ANOMALY_LIMIT));

    DataStream<? extends GenealogTuple> blackoutStream =
        sourceStream
            .keyBy(GL.key(s -> s.getKey(), String.class))
            .window(
                SlidingEventTimeWindows.of(
                    BLACKOUT_AGGREGATE_WINDOW_SIZE, BLACKOUT_AGGREGATE_WINDOW_SLIDE))
            .aggregate(GL.aggregate(new MeterUsageAggregateFunction()))
            .filter(GL.filter(s -> s.getValue() == 0))
            .windowAll(
                SlidingEventTimeWindows.of(
                    BLACKOUT_AGGREGATE_WINDOW_SIZE, BLACKOUT_AGGREGATE_WINDOW_SLIDE))
            .aggregate(GL.aggregate(new CountAggregateFunction()))
            .slotSharingGroup(settings.secondSlotSharingGroup())
            .filter(GL.filter(c -> c.getCount() > BLACKOUT_MAX_COUNT));

    settings
        .genealogActivator()
        .activate(
            Arrays.asList(
                ProvenanceActivator.convert(blackoutStream),
                ProvenanceActivator.convert(anomalyStream)),
            Arrays.asList("BLACKOUT", "ANOMALY"),
            settings,
            BLACKOUT_AGGREGATE_WINDOW_SIZE.toMilliseconds()
                + BLACKOUT_AGGREGATE_WINDOW_SIZE.toMilliseconds(),
            timestampConverter);
    env.execute("SmartGridCombined");
  }
}
