package io.palyvos.provenance.usecases.smartgrid.provenance.queries;

import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_AGGR_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_AGGR_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_JOIN_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_LIMIT;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_AGGREGATE_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_AGGREGATE_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_MAX_COUNT;

import io.palyvos.provenance.util.ProvenanceActivator;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.usecases.smartgrid.provenance.CountAggregateFunction;
import io.palyvos.provenance.usecases.smartgrid.provenance.MeterUsageAggregateFunction;
import io.palyvos.provenance.usecases.smartgrid.provenance.SmartGridSourceGL;
import io.palyvos.provenance.usecases.smartgrid.provenance.SmartGridTupleGL;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.FlinkSerializerActivator.SerializerRegistry;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.util.TimestampConverter;
import java.util.Arrays;
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

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final TimestampConverter timestampConverter = (ts) -> Time.hours(ts).toMilliseconds();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setMaxParallelism(settings.maxParallelism());

    SerializerRegistry serializerRegistry = FlinkSerializerActivator.PROVENANCE_OPTIMIZED
        .activate(env, settings);
    serializerRegistry.register(SmartGridTupleGL.class,
        new SmartGridTupleGL.KryoSerializer(serializerRegistry.genealogDataSerializer()));

    DataStream<SmartGridTupleGL> sourceStream =
        env.addSource(new SmartGridSourceGL(settings))
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<SmartGridTupleGL>() {
                  @Override
                  public long extractAscendingTimestamp(SmartGridTupleGL element) {
                    return timestampConverter.apply(element.getTimestamp());
                  }
                })
            .setParallelism(1)
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .returns(SmartGridTupleGL.class)
            .setParallelism(env.getParallelism());

    DataStream<SmartGridTupleGL> midnightStream =
        sourceStream.filter(t -> ((t.getTimestamp() + 1) % 24 == 0));

    DataStream<? extends GenealogTuple> anomalyStream =
        sourceStream
            .keyBy(s -> s.getKey())
            .window(SlidingEventTimeWindows.of(ANOMALY_AGGR_WINDOW_SIZE, ANOMALY_AGGR_WINDOW_SLIDE))
            .aggregate(new MeterUsageAggregateFunction(settings.aggregateStrategySupplier()))
            .slotSharingGroup(settings.secondSlotSharingGroup())
            .filter(s -> s.getValue() > ANOMALY_LIMIT)
            .join(midnightStream)
            .where(s -> s.getKey())
            .equalTo(s -> s.getKey())
            .window(TumblingEventTimeWindows.of(ANOMALY_JOIN_WINDOW_SIZE))
            .apply(new SmartGridAnomaly.AnomalyJoinFunctionGL())
            .filter(s -> s.getValue() > ANOMALY_LIMIT);

    DataStream<? extends GenealogTuple> blackoutStream =
        sourceStream
            .keyBy(s -> s.getKey())
            .window(
                SlidingEventTimeWindows.of(
                    BLACKOUT_AGGREGATE_WINDOW_SIZE, BLACKOUT_AGGREGATE_WINDOW_SLIDE))
            .aggregate(new MeterUsageAggregateFunction(settings.aggregateStrategySupplier()))
            .filter(s -> s.getValue() == 0)
            .windowAll(
                SlidingEventTimeWindows.of(
                    BLACKOUT_AGGREGATE_WINDOW_SIZE, BLACKOUT_AGGREGATE_WINDOW_SLIDE))
            .aggregate(new CountAggregateFunction(settings.aggregateStrategySupplier()))
            .slotSharingGroup(settings.secondSlotSharingGroup())
            .filter(c -> c.getCount() > BLACKOUT_MAX_COUNT);

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
