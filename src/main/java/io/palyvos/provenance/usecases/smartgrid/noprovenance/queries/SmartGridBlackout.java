package io.palyvos.provenance.usecases.smartgrid.noprovenance.queries;

import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_AGGREGATE_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_AGGREGATE_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.BLACKOUT_MAX_COUNT;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.CountAggregateFunction;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.MeterUsageAggregateFunction;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.SmartGridSource;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.SmartGridTuple;
import io.palyvos.provenance.util.LatencyLoggingSink;
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

    env.addSource(new SmartGridSource(settings))
        .assignTimestampsAndWatermarks(
            new AscendingTimestampExtractor<SmartGridTuple>() {
              @Override
              public long extractAscendingTimestamp(SmartGridTuple element) {
                return Time.hours(element.getTimestamp()).toMilliseconds();
              }
            })
        .keyBy(s -> s.getKey())
        .window(SlidingEventTimeWindows.of(BLACKOUT_AGGREGATE_WINDOW_SIZE,
            BLACKOUT_AGGREGATE_WINDOW_SLIDE))
        .aggregate(new MeterUsageAggregateFunction())
        .filter(s -> s.getValue() == 0)
        .windowAll(SlidingEventTimeWindows.of(BLACKOUT_AGGREGATE_WINDOW_SIZE,
            BLACKOUT_AGGREGATE_WINDOW_SLIDE))
        .aggregate(new CountAggregateFunction())
        .slotSharingGroup(settings.secondSlotSharingGroup())
        .filter(c -> c.getCount() > BLACKOUT_MAX_COUNT)
        .addSink(LatencyLoggingSink.newInstance(settings));

    env.execute("SmartGridBlackout");
  }
}
