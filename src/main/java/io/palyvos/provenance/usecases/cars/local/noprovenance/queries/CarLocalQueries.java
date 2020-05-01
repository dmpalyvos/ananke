package io.palyvos.provenance.usecases.cars.local.noprovenance.queries;


import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.CYCLIST_COUNTING_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.CYCLIST_COUNTING_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.PEDESTRIAN_PASSING_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.PEDESTRIAN_PASSING_WINDOW_SIZE;

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
import io.palyvos.provenance.util.LatencyLoggingSink;
import java.io.Serializable;
import java.util.function.Function;
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
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setMaxParallelism(settings.maxParallelism());
    env.getConfig().enableObjectReuse();

    FlinkSerializerActivator.NOPROVENANCE.activate(env, settings)
        .register(CarLocalInputTuple.class, new CarLocalInputTuple.KryoSerializer())
        .register(LidarImageContainer.class, new LidarImageContainer.KryoSerializer());

    // connect to source
    SingleOutputStreamOperator<CarLocalInputTuple>
        LidarLeftRightTimestamped =
        env.addSource(new CarLocalSocketSource(
            settings.sourceIP(), settings.sourcePort(), 10, 500, settings))
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<CarLocalInputTuple>() {
                  @Override
                  public long extractAscendingTimestamp(CarLocalInputTuple t) {
                    return t.f0;
                  }
                });

    // ----------------------------------------------------------------------------
    // ----------------------------- QUERY 1 --------------------------------------
    // ----------------------------------------------------------------------------

    DataStream<Tuple4<String, String, Annotation3D, Long>> pedestrians = LidarLeftRightTimestamped
        .flatMap(
            new CarLocalPedestriansToSingleObject()) // payload_type, object_id, tuple4(object_name,x,y,z)
        .name("flatMap: to single object instances")
        .filter(new CarLocalFilterPedestrians())
        .name("filter: pedestrian");

    pedestrians
        .join(pedestrians)
        .where(
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
            })
        .equalTo(right -> right.f0 + right.f1)
        .window(SlidingEventTimeWindows
            .of(PEDESTRIAN_PASSING_WINDOW_SIZE, PEDESTRIAN_PASSING_WINDOW_ADVANCE))
        .apply(new CarLocalPedestriansPredicate())
        .addSink(LatencyLoggingSink.newInstance("CROSSEDPEDESTRIANS",
            (Serializable & Function<Tuple2<String, Long>, Long>) t -> t.f1,
            settings))
        .name("IDs of pedestrians that crossed");

    // ----------------------------------------------------------------------------
    // ----------------------------- QUERY 2 --------------------------------------
    // ----------------------------------------------------------------------------

    LidarLeftRightTimestamped
        .flatMap(new CarLocalCyclistsToSingleObject())
        .name("flatMap: to single object instances")
        .filter(new CarLocalFilterBicycles())
        .name("filter: bicycles")
        .keyBy(t -> t.f0)
        .window(SlidingEventTimeWindows
            .of(CYCLIST_COUNTING_WINDOW_SIZE, CYCLIST_COUNTING_WINDOW_ADVANCE))
        .aggregate(new CarLocalCount())
        .name("agg: count")
        .filter(new CarLocalFilterCyclesInFront())
        .addSink(LatencyLoggingSink.newInstance("CYCLESINFRONT",
            (Serializable & Function<Tuple3<String, Integer, Long>, Long>) t -> t.f2,
            settings))
        .name("cyclists that are in front of vehicle");

    env.execute("Intra-vehicular queries over camera and LiDAR");
  }

}

