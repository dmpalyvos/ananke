package io.palyvos.provenance.usecases.smartgrid.provenance.queries;

import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_AGGREGATE_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_AGGREGATE_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_MAX_COUNT;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.usecases.smartgrid.provenance.CountAggregateFunction;
import io.palyvos.provenance.usecases.smartgrid.provenance.MeterUsageAggregateFunction;
import io.palyvos.provenance.usecases.smartgrid.provenance.SmartGridSourceGL;
import io.palyvos.provenance.usecases.smartgrid.provenance.SmartGridTupleGL;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogDataSerializer;
import io.palyvos.provenance.genealog.GenealogFileSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SmartGridBlackout {

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

    env.addSource(new SmartGridSourceGL(settings))
        .assignTimestampsAndWatermarks(
            new AscendingTimestampExtractor<SmartGridTupleGL>() {
              @Override
              public long extractAscendingTimestamp(SmartGridTupleGL element) {
                return Time.hours(element.getTimestamp()).toMilliseconds();
              }
            })
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
        .filter(c -> c.getCount() > BLACKOUT_MAX_COUNT)
        .addSink(GenealogFileSink.newInstance(settings));

    env.execute("SmartGridBlackout");
  }
}
