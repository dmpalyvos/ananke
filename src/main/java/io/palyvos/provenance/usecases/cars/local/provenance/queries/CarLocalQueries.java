package io.palyvos.provenance.usecases.cars.local.provenance.queries;


import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.CYCLIST_COUNTING_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.CYCLIST_COUNTING_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.PEDESTRIAN_PASSING_WINDOW_ADVANCE;
import static io.palyvos.provenance.usecases.cars.local.CarLocalConstants.PEDESTRIAN_PASSING_WINDOW_SIZE;

import io.palyvos.provenance.usecases.cars.Tuple2GL;
import io.palyvos.provenance.usecases.cars.Tuple3GL;
import io.palyvos.provenance.usecases.cars.Tuple4GL;
import io.palyvos.provenance.usecases.cars.local.LidarImageContainer;
import io.palyvos.provenance.usecases.cars.local.LidarImageContainer.Annotation3D;
import io.palyvos.provenance.usecases.cars.local.provenance.CarLocalCountGL;
import io.palyvos.provenance.usecases.cars.local.provenance.CarLocalCyclistsToSingleObjectGL;
import io.palyvos.provenance.usecases.cars.local.provenance.CarLocalFilterBicyclesGL;
import io.palyvos.provenance.usecases.cars.local.provenance.CarLocalFilterCyclesInFrontGL;
import io.palyvos.provenance.usecases.cars.local.provenance.CarLocalFilterPedestriansGL;
import io.palyvos.provenance.usecases.cars.local.provenance.CarLocalInputTupleGL;
import io.palyvos.provenance.usecases.cars.local.provenance.CarLocalPedestriansPredicateGL;
import io.palyvos.provenance.usecases.cars.local.provenance.CarLocalPedestriansToSingleObjectGL;
import io.palyvos.provenance.usecases.cars.local.provenance.CarLocalSocketSourceGL;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.FlinkSerializerActivator.SerializerRegistry;
import io.palyvos.provenance.util.ProvenanceActivator;
import io.palyvos.provenance.util.TimestampConverter;
import java.util.Arrays;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

public class CarLocalQueries {


  public static void main(String[] args) throws Exception {

    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final TimestampConverter timestampConverter = ts -> ts;

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setMaxParallelism(settings.maxParallelism());
    env.getConfig().enableObjectReuse();

    SerializerRegistry serializerRegistry = FlinkSerializerActivator.PROVENANCE_OPTIMIZED
        .activate(env, settings);
    serializerRegistry
        .register(CarLocalInputTupleGL.class,
            new CarLocalInputTupleGL.KryoSerializer(serializerRegistry.genealogDataSerializer()))
        .register(
            LidarImageContainer.class, new LidarImageContainer.KryoSerializer());

    // connect to source
    SingleOutputStreamOperator<CarLocalInputTupleGL>
        LidarLeftRightTimestamped =
        env.addSource(new CarLocalSocketSourceGL(
            settings.sourceIP(), settings.sourcePort(), 10, 500, settings))
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<CarLocalInputTupleGL>() {
                  @Override
                  public long extractAscendingTimestamp(CarLocalInputTupleGL t) {
                    return t.f0;
                  }
                })
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .returns(CarLocalInputTupleGL.class);

    // ----------------------------------------------------------------------------
    // ----------------------------- QUERY 1 --------------------------------------
    // ----------------------------------------------------------------------------

    DataStream<Tuple4GL<String, String, Annotation3D, Long>> pedestrians = LidarLeftRightTimestamped
        .flatMap(
            new CarLocalPedestriansToSingleObjectGL()) // payload_type, object_id, tuple4(object_name,x,y,z)
        .name("flatMap: to single object instances")
        .filter(new CarLocalFilterPedestriansGL())
        .name("filter: pedestrian");

    DataStream<Tuple2GL<String, Long>> crossedPedestrians =
        pedestrians
            .join(pedestrians)
            .where(
                (KeySelector<Tuple4GL<String, String, Annotation3D, Long>, String>) left -> {
                  if (left.f0.equals("ring_front_left")) {
                    return "ring_front_right" + left.f1;
                  } else {
                    return "ring_front_left" + left.f1;
                  }
                })
            .equalTo(right -> right.f0 + right.f1)
            .window(SlidingEventTimeWindows
                .of(PEDESTRIAN_PASSING_WINDOW_SIZE, PEDESTRIAN_PASSING_WINDOW_ADVANCE))
            .apply(new CarLocalPedestriansPredicateGL());

    // ----------------------------------------------------------------------------
    // ----------------------------- QUERY 2 --------------------------------------
    // ----------------------------------------------------------------------------

    DataStream<Tuple3GL<String, Integer, Long>> cyclesInFront =
        LidarLeftRightTimestamped
            .flatMap(new CarLocalCyclistsToSingleObjectGL())
            .name("flatMap: to single object instances")
            .filter(new CarLocalFilterBicyclesGL())
            .name("filter: bicycles")
            .keyBy(t -> t.f0)
            .window(SlidingEventTimeWindows
                .of(CYCLIST_COUNTING_WINDOW_SIZE, CYCLIST_COUNTING_WINDOW_ADVANCE))
            .aggregate(new CarLocalCountGL(settings.aggregateStrategySupplier()))
            .name("agg: count")
            .filter(new CarLocalFilterCyclesInFrontGL());

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

