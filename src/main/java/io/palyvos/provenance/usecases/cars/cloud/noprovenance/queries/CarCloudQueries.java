package io.palyvos.provenance.usecases.cars.cloud.noprovenance.queries;

import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.AREA_MIN_COUNT;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.AVERAGE_SPEED_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.AVERAGE_SPEED_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.INSTANT_SPEED_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.INSTANT_SPEED_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.TIME_AREA_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.TIME_AREA_WINDOW_SIZE;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.usecases.cars.cloud.CarCloudInputTuple;
import io.palyvos.provenance.usecases.cars.cloud.CarCloudSocketSource;
import io.palyvos.provenance.usecases.cars.cloud.noprovenance.CarCloudAverageSpeed;
import io.palyvos.provenance.usecases.cars.cloud.noprovenance.CarCloudCheckRegion;
import io.palyvos.provenance.usecases.cars.cloud.noprovenance.CarCloudCount;
import io.palyvos.provenance.usecases.cars.cloud.noprovenance.CarCloudCurrentSpeed;
import io.palyvos.provenance.usecases.cars.cloud.noprovenance.CarCloudFilterAverageSpeed;
import io.palyvos.provenance.usecases.cars.cloud.noprovenance.CarCloudFilterOccurences;
import io.palyvos.provenance.usecases.cars.cloud.noprovenance.CarCloudFirstFilter;
import io.palyvos.provenance.usecases.cars.cloud.noprovenance.CarCloudGetWindowFirstLast;
import io.palyvos.provenance.usecases.cars.cloud.noprovenance.CarCloudRemoveTimestamps;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.LatencyLoggingSink;
import java.io.Serializable;
import java.util.function.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

public class CarCloudQueries {


  public static void main(String[] args) throws Exception {

    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();

    FlinkSerializerActivator.NOPROVENANCE.activate(env, settings)
        .register(CarCloudInputTuple.class, new CarCloudInputTuple.KryoSerializer());

    // connect to source
    DataStreamSource<CarCloudInputTuple> ts_carID_lat_lon =
        env.addSource(new CarCloudSocketSource(settings.sourceIP(), settings.sourcePort(), 10, 500,
            settings));

    DataStream<CarCloudInputTuple> ts_carID_lat_lon_timestamped =
        ts_carID_lat_lon.assignTimestampsAndWatermarks(
            new AscendingTimestampExtractor<CarCloudInputTuple>() {
              @Override
              public long extractAscendingTimestamp(CarCloudInputTuple t) {
                return t.f0;
              }
            });

    // ----------------------------------------------------------------------
    // ------------------------- QUERY 1 ------------------------------------
    // ----------------------------------------------------------------------

    //                 key by car_ID
    ts_carID_lat_lon_timestamped
        .keyBy(t -> t.f1)
        .window(SlidingEventTimeWindows.of(INSTANT_SPEED_WINDOW_SIZE, INSTANT_SPEED_WINDOW_ADVANCE))
        .aggregate(new CarCloudGetWindowFirstLast())
        .name("agg: Get first and last tuple in window")
        .filter(new CarCloudFirstFilter())
        .map(new CarCloudCurrentSpeed())
        .name("map: Get current speed")
        //                 id, current speed
        .keyBy(t -> t.f0, TypeInformation.of(Integer.class))
        .window(SlidingEventTimeWindows.of(AVERAGE_SPEED_WINDOW_SIZE, AVERAGE_SPEED_WINDOW_ADVANCE))
        .aggregate(new CarCloudAverageSpeed())
        .name("agg: Average speed")
        .filter(new CarCloudFilterAverageSpeed())
        .addSink(LatencyLoggingSink.newInstance("HIGHSPEED",
            (Serializable & Function<Tuple2<Double, Long>, Long>) t -> t.f1,
            settings))
        .name("sink: cars with average speed above 70km/h");

    // ----------------------------------------------------------------------
    // ------------------------- QUERY 2 ------------------------------------
    // ----------------------------------------------------------------------

    ts_carID_lat_lon_timestamped
        .map(new CarCloudRemoveTimestamps())
        .name("map: remove timestamps")
        .filter(new CarCloudCheckRegion())
        .name("filter: check region")
        //                key by car_ID
        .keyBy(t -> t.f0)
        .window(SlidingEventTimeWindows.of(TIME_AREA_WINDOW_SIZE, TIME_AREA_WINDOW_ADVANCE))
        .aggregate(new CarCloudCount())
        .name("agg: count")
        .filter(new CarCloudFilterOccurences())
        .name(String.format("filter: > %d occurrences", AREA_MIN_COUNT))
        .addSink(LatencyLoggingSink.newInstance("INAREA",
            (Serializable & Function<Tuple3<Integer, Integer, Long>, Long>) t -> t.f2,
            settings))
        .name("sink: cars in the target region for a certain amount of time");

    env.execute("Inter-vehicular cloud queries");
  }

}
