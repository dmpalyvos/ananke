package io.palyvos.provenance.usecases.cars.cloud.provenance2.queries;

import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.AREA_MIN_COUNT;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.AVERAGE_SPEED_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.AVERAGE_SPEED_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.INSTANT_SPEED_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.INSTANT_SPEED_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.TIME_AREA_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.cloud.CarCloudConstants.TIME_AREA_WINDOW_SIZE;

import io.palyvos.provenance.util.ProvenanceActivator;
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
import io.palyvos.provenance.util.TimestampConverter;
import io.palyvos.provenance.ananke.functions.ProvenanceFunctionFactory;
import io.palyvos.provenance.ananke.functions.ProvenanceTupleContainer;
import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

public class CarCloudQueries {

  public static void main(String[] args) throws Exception {

    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final TimestampConverter timestampConverter = ts -> ts;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setMaxParallelism(settings.maxParallelism());
    //    env.disableOperatorChaining();
    env.getConfig().enableObjectReuse();

    final ProvenanceFunctionFactory GL =
        new ProvenanceFunctionFactory(settings.aggregateStrategySupplier());

    FlinkSerializerActivator.PROVENANCE_TRANSPARENT.activate(env, settings)
        .register(CarCloudInputTuple.class, new CarCloudInputTuple.KryoSerializer());

    // connect to source
    DataStreamSource<CarCloudInputTuple> ts_carID_lat_lon =
        env.addSource(new CarCloudSocketSource(settings.sourceIP(), settings.sourcePort(), 10, 500,
            settings));

    DataStream<ProvenanceTupleContainer<CarCloudInputTuple>> ts_carID_lat_lon_timestamped =
        ts_carID_lat_lon
            .name("CAR_LOCAL_SOURCE")
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<CarCloudInputTuple>() {
                  @Override
                  public long extractAscendingTimestamp(CarCloudInputTuple t) {
                    return timestampConverter.apply(t.f0);
                  }
                })
            .map(
                GL.initMap(
                    (Serializable & Function<CarCloudInputTuple, Long>) t -> t.f0,
                        (Serializable & Function<CarCloudInputTuple, Long>) t -> t.getStimulus()))
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .returns(new TypeHint<ProvenanceTupleContainer<CarCloudInputTuple>>() {});

    // ----------------------------------------------------------------------
    // ------------------------- QUERY 1 ------------------------------------
    // ----------------------------------------------------------------------

    //                 key by car_ID
    SingleOutputStreamOperator<ProvenanceTupleContainer<Tuple2<Double, Long>>> fastCarsStream =
        ts_carID_lat_lon_timestamped
            .keyBy(GL.key(t -> t.f1), TypeInformation.of(Integer.class))
            .window(
                SlidingEventTimeWindows.of(INSTANT_SPEED_WINDOW_SIZE, INSTANT_SPEED_WINDOW_ADVANCE))
            .aggregate(GL.aggregate(new CarCloudGetWindowFirstLast()))
            .name("agg: Get first and last tuple in window")
            .filter(GL.filter(new CarCloudFirstFilter()))
            .map(GL.map(new CarCloudCurrentSpeed()))
            .name("map: Get current speed")
            //                 id, current speed
            .keyBy(GL.key(t -> t.f0), TypeInformation.of(Integer.class))
            .window(
                SlidingEventTimeWindows.of(AVERAGE_SPEED_WINDOW_SIZE, AVERAGE_SPEED_WINDOW_ADVANCE))
            .aggregate(
                GL.aggregate(new CarCloudAverageSpeed()))
            .name("agg: Average speed")
            .filter(GL.filter(new CarCloudFilterAverageSpeed()));

    // ----------------------------------------------------------------------
    // ------------------------- QUERY 2 ------------------------------------
    // ----------------------------------------------------------------------

    SingleOutputStreamOperator<ProvenanceTupleContainer<Tuple3<Integer, Integer, Long>>> areaCarsStream =
        ts_carID_lat_lon_timestamped
            .map(GL.map(new CarCloudRemoveTimestamps()))
            .name("map: remove timestamps")
            .filter(GL.filter(new CarCloudCheckRegion()))
            .name("filter: check region")

            //                key by car_ID
            .keyBy(GL.key(t -> t.f0), TypeInformation.of(Integer.class))
            .window(SlidingEventTimeWindows.of(TIME_AREA_WINDOW_SIZE, TIME_AREA_WINDOW_ADVANCE))
            .aggregate(
                GL.aggregate(new CarCloudCount()))
            .name("agg: count")
            .filter(GL.filter(new CarCloudFilterOccurences()))
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
