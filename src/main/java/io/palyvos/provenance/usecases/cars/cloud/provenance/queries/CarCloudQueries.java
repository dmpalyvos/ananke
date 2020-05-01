package io.palyvos.provenance.usecases.cars.cloud.provenance.queries;

import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.AREA_MIN_COUNT;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.AVERAGE_SPEED_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.AVERAGE_SPEED_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.INSTANT_SPEED_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.INSTANT_SPEED_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.TIME_AREA_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.TIME_AREA_WINDOW_SIZE;

import io.palyvos.provenance.usecases.cars.Tuple2GL;
import io.palyvos.provenance.usecases.cars.Tuple3GL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudAverageSpeedGL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudCheckRegionGL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudCountGL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudCurrentSpeedGL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudFilterAverageSpeedGL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudFilterOccurencesGL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudFirstFilterGL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudGetWindowFirstLastGL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudInputTupleGL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudRemoveTimestampsGL;
import io.palyvos.provenance.usecases.cars.cloud.provenance.CarCloudSocketSourceGL;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.FlinkSerializerActivator.SerializerRegistry;
import io.palyvos.provenance.util.ProvenanceActivator;
import io.palyvos.provenance.util.TimestampConverter;
import java.util.Arrays;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

public class CarCloudQueries {


  public static void main(String[] args) throws Exception {

    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final TimestampConverter timestampConverter = ts -> ts;

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setMaxParallelism(settings.maxParallelism());
    env.getConfig().enableObjectReuse();

    SerializerRegistry serializerRegistry = FlinkSerializerActivator.PROVENANCE_OPTIMIZED
        .activate(env, settings);
    serializerRegistry.register(CarCloudInputTupleGL.class,
        new CarCloudInputTupleGL.KryoSerializer(serializerRegistry.genealogDataSerializer()));

    // connect to source
    DataStreamSource<CarCloudInputTupleGL> ts_carID_lat_lon =
        env.addSource(
            new CarCloudSocketSourceGL(settings.sourceIP(), settings.sourcePort(), 10, 500,
                settings));

    DataStream<CarCloudInputTupleGL> ts_carID_lat_lon_timestamped =
        ts_carID_lat_lon.assignTimestampsAndWatermarks(
            new AscendingTimestampExtractor<CarCloudInputTupleGL>() {
              @Override
              public long extractAscendingTimestamp(CarCloudInputTupleGL t) {
                return t.f0;
              }
            })
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .returns(CarCloudInputTupleGL.class);

    // ----------------------------------------------------------------------
    // ------------------------- QUERY 1 ------------------------------------
    // ----------------------------------------------------------------------

    //                 key by car_ID
    DataStream<Tuple2GL<Double, Long>> fastCarsStream = ts_carID_lat_lon_timestamped
        .keyBy(t -> t.f1)
        .window(SlidingEventTimeWindows.of(INSTANT_SPEED_WINDOW_SIZE, INSTANT_SPEED_WINDOW_ADVANCE))
        .aggregate(new CarCloudGetWindowFirstLastGL(settings.aggregateStrategySupplier()))
        .name("agg: Get first and last tuple in window")
        .filter(new CarCloudFirstFilterGL())
        .map(new CarCloudCurrentSpeedGL())
        .name("map: Get current speed")
        //                 id, current speed
        .keyBy(t -> t.f0, TypeInformation.of(Integer.class))
        .window(SlidingEventTimeWindows.of(AVERAGE_SPEED_WINDOW_SIZE, AVERAGE_SPEED_WINDOW_ADVANCE))
        .aggregate(new CarCloudAverageSpeedGL(settings.aggregateStrategySupplier()))
        .name("agg: Average speed")
        .filter(new CarCloudFilterAverageSpeedGL());

    // ----------------------------------------------------------------------
    // ------------------------- QUERY 2 ------------------------------------
    // ----------------------------------------------------------------------

    DataStream<Tuple3GL<Integer, Integer, Long>> areaCarsStream =
        ts_carID_lat_lon_timestamped
            .map(new CarCloudRemoveTimestampsGL())
            .name("map: remove timestamps")
            .filter(new CarCloudCheckRegionGL())
            .name("filter: check region")
            //                key by car_ID
            .keyBy(t -> t.f0)
            .window(SlidingEventTimeWindows.of(TIME_AREA_WINDOW_SIZE, TIME_AREA_WINDOW_ADVANCE))
            .aggregate(new CarCloudCountGL(settings.aggregateStrategySupplier()))
            .name("agg: count")
            .filter(new CarCloudFilterOccurencesGL())
            .name(String.format("filter: > %d occurrences", AREA_MIN_COUNT));

    settings
        .genealogActivator()
        .activate(
            Arrays.asList(
                ProvenanceActivator.convert(fastCarsStream),
                ProvenanceActivator.convert(areaCarsStream)),
            Arrays.asList("HIGHSPEED", "INAREA"),
            settings,
            Math.max(
                TIME_AREA_WINDOW_SIZE.toMilliseconds(),
                AVERAGE_SPEED_WINDOW_SIZE.toMilliseconds()
                    + INSTANT_SPEED_WINDOW_SIZE.toMilliseconds()),
            timestampConverter);

    env.execute("Inter-vehicular cloud queries");
  }

}
