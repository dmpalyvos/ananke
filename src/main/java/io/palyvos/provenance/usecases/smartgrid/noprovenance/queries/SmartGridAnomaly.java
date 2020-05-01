package io.palyvos.provenance.usecases.smartgrid.noprovenance.queries;

import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_AGGR_STREAM_KEY;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_AGGR_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_AGGR_WINDOW_SLIDE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_JOIN_STREAM_KEY;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_JOIN_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.smartgrid.SmartGridConstants.ANOMALY_LIMIT;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.usecases.smartgrid.AnomalyOutputSelector;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.MeterUsageAggregateFunction;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.SmartGridSource;
import io.palyvos.provenance.usecases.smartgrid.noprovenance.SmartGridTuple;
import io.palyvos.provenance.util.LatencyLoggingSink;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SmartGridAnomaly {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();

    SplitStream<SmartGridTuple> splitStream = env.addSource(new SmartGridSource(settings))
        .assignTimestampsAndWatermarks(
            new AscendingTimestampExtractor<SmartGridTuple>() {
              @Override
              public long extractAscendingTimestamp(SmartGridTuple element) {
                return Time.hours(element.getTimestamp()).toMilliseconds();
              }
            })
        // Multiplex + Filter
        .split(new AnomalyOutputSelector<>());

    DataStream<SmartGridTuple> nonAggregatedStream = splitStream.select(ANOMALY_JOIN_STREAM_KEY);

    splitStream.select(ANOMALY_AGGR_STREAM_KEY)
        .keyBy(s -> s.getKey())
        .window(SlidingEventTimeWindows.of(ANOMALY_AGGR_WINDOW_SIZE, ANOMALY_AGGR_WINDOW_SLIDE))
        .aggregate(new MeterUsageAggregateFunction())
        .slotSharingGroup(settings.secondSlotSharingGroup())
        .filter(s -> s.getValue() > ANOMALY_LIMIT)
        .join(nonAggregatedStream)
        .where(s -> s.getKey()).equalTo(s -> s.getKey())
        .window(TumblingEventTimeWindows.of(ANOMALY_JOIN_WINDOW_SIZE))
        .apply(new AnomalyJoinFunction())
        .filter(s -> s.getValue() > ANOMALY_LIMIT)
        .addSink(LatencyLoggingSink.newInstance(settings))
        .setParallelism(settings.sinkParallelism());

    env.execute("SmartGridAnomaly");
  }

  public static class AnomalyJoinFunction implements JoinFunction<SmartGridTuple,
      SmartGridTuple,
      SmartGridTuple> {

    @Override
    public SmartGridTuple join(SmartGridTuple first, SmartGridTuple second) throws Exception {
      SmartGridTuple result = new SmartGridTuple(first.getKey(), Math.max(first.getTimestamp(),
          second.getTimestamp()),
          Math.max(first.getStimulus(), second.getStimulus()),
          Math.abs(first.getValue() - second.getValue()));
      return result;
    }
  }

}
