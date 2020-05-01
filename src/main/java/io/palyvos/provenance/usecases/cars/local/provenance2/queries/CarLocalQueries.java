package io.palyvos.provenance.usecases.cars.local.provenance2.queries;

import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.CYCLIST_COUNTING_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.CYCLIST_COUNTING_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.PEDESTRIAN_PASSING_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.PEDESTRIAN_PASSING_WINDOW_SIZE;

import io.palyvos.provenance.ananke.functions.ProvenanceFunctionFactory;
import io.palyvos.provenance.ananke.functions.ProvenanceTupleContainer;
import io.palyvos.provenance.usecases.cars.local.CarLocalInputTuple;
import io.palyvos.provenance.usecases.cars.local.CarLocalSocketSource;
import io.palyvos.provenance.usecases.cars.local.LidarImageContainer;
import io.palyvos.provenance.usecases.cars.local.LidarImageContainer.Annotation3D;
import io.palyvos.provenance.usecases.cars.local.noprovenance.CarLocalCount;
import io.palyvos.provenance.usecases.cars.local.noprovenance.CarLocalCyclistsToSingleObject;
import io.palyvos.provenance.usecases.cars.local.noprovenance.CarLocalFilterBicycles;
import io.palyvos.provenance.usecases.cars.local.noprovenance.CarLocalFilterCyclesInFront;
import io.palyvos.provenance.usecases.cars.local.noprovenance.CarLocalFilterPedestrians;
import io.palyvos.provenance.usecases.cars.local.noprovenance.CarLocalPedestriansPredicate;
import io.palyvos.provenance.usecases.cars.local.noprovenance.CarLocalPedestriansToSingleObject;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.ProvenanceActivator;
import io.palyvos.provenance.util.TimestampConverter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

public class CarLocalQueries {


  public static void main(String[] args) throws Exception {

    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final TimestampConverter timestampConverter = ts -> ts;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setMaxParallelism(settings.maxParallelism());
    env.getConfig().enableObjectReuse();

    final ProvenanceFunctionFactory GL = new ProvenanceFunctionFactory(
        settings.aggregateStrategySupplier());

    FlinkSerializerActivator.PROVENANCE_TRANSPARENT.activate(env, settings)
        .register(CarLocalInputTuple.class, new CarLocalInputTuple.KryoSerializer())
        .register(LidarImageContainer.class, new LidarImageContainer.KryoSerializer());

    // connect to source
    SingleOutputStreamOperator<ProvenanceTupleContainer<CarLocalInputTuple>>
        LidarLeftRightTimestamped =
        env
            .addSource(new CarLocalSocketSource(
                settings.sourceIP(), settings.sourcePort(), 10, 500, settings))
            .name("CAR_LOCAL_SOURCE")
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<CarLocalInputTuple>() {
                  @Override
                  public long extractAscendingTimestamp(CarLocalInputTuple t) {
                    return timestampConverter.apply(t.f0);
                  }
                })
            .map(
                GL.initMap(
                    (Serializable & Function<CarLocalInputTuple, Long>) t -> t.f0,
                    (Serializable & Function<CarLocalInputTuple, Long>)
                        t -> t.getStimulus()))
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .returns(new TypeHint<ProvenanceTupleContainer<CarLocalInputTuple>>() {
            });

    // ----------------------------------------------------------------------------
    // ----------------------------- QUERY 1 --------------------------------------
    // ----------------------------------------------------------------------------

    DataStream<ProvenanceTupleContainer<Tuple4<String, String, Annotation3D, Long>>> pedestrians = LidarLeftRightTimestamped
        .flatMap(GL.flatMap(
            new CarLocalPedestriansToSingleObject())) // payload_type, object_id, tuple4(object_name,x,y,z)
        .name("flatMap: to single object instances")
        .filter(GL.filter(new CarLocalFilterPedestrians()))
        .name("filter: pedestrian");

    DataStream<ProvenanceTupleContainer<Tuple2<String, Long>>> crossedPedestrians = pedestrians
        .join(pedestrians)
        .where(GL.key(
            new KeySelector<Tuple4<String, String, Annotation3D, Long>, String>() {
              @Override
              public String getKey(
                  Tuple4<String, String, Annotation3D, Long> left)
                  throws Exception {
                if (left.f0.equals("ring_front_left")) {
                  return "ring_front_right" + left.f1;
                } else {
                  return "ring_front_left" + left.f1;
                }
              }
            }, String.class))
        .equalTo(GL.key(right -> right.f0 + right.f1, String.class))
        .window(SlidingEventTimeWindows
            .of(PEDESTRIAN_PASSING_WINDOW_SIZE, PEDESTRIAN_PASSING_WINDOW_ADVANCE))
        .apply(GL.join(new CarLocalPedestriansPredicate()));

    // ----------------------------------------------------------------------------
    // ----------------------------- QUERY 2 --------------------------------------
    // ----------------------------------------------------------------------------

    DataStream<ProvenanceTupleContainer<Tuple3<String, Integer, Long>>> cyclesInFront = LidarLeftRightTimestamped
        .flatMap(GL.flatMap(new CarLocalCyclistsToSingleObject()))
        .name("flatMap: to single object instances")
        .filter(GL.filter(new CarLocalFilterBicycles()))
        .name("filter: bicycles")
        .keyBy(GL.key(t -> t.f0), TypeInformation.of(String.class))
        .window(SlidingEventTimeWindows
            .of(CYCLIST_COUNTING_WINDOW_SIZE, CYCLIST_COUNTING_WINDOW_ADVANCE))
        .aggregate(GL.aggregate(new CarLocalCount()))
        .name("agg: count")
        .filter(GL.filter(new CarLocalFilterCyclesInFront()));

    settings
        .genealogActivator()
        .activate(
            Arrays.asList(
                ProvenanceActivator.convert(cyclesInFront),
                ProvenanceActivator.convert(crossedPedestrians)),
            Arrays.asList("CYCLESINFRONT",
                "CROSSEDPEDESTRIANS"),
            settings,
            Math.max(PEDESTRIAN_PASSING_WINDOW_SIZE.toMilliseconds(),
                CYCLIST_COUNTING_WINDOW_SIZE.toMilliseconds()),
            timestampConverter);

    env.execute("Intra-vehicular queries over camera and LiDAR");
  }
}


