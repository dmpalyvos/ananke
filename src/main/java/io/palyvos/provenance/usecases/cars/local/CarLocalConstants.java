package io.palyvos.provenance.usecases.cars.local;

import org.apache.flink.streaming.api.windowing.time.Time;

public class CarLocalConstants {

    public static final Time PEDESTRIAN_PASSING_WINDOW_SIZE = Time.seconds(2);
    public static final Time PEDESTRIAN_PASSING_WINDOW_ADVANCE = Time.seconds(1);
    public static final int CYCLIST_Y_AREA = 20;
    public static final int CYCLIST_Z_AREA = 20;
    public static final Time CYCLIST_COUNTING_WINDOW_SIZE = Time.seconds(6);
    public static final Time CYCLIST_COUNTING_WINDOW_ADVANCE = Time.seconds(3);
    public static final int CYCLIST_MIN_NUMBER = 11;

    private CarLocalConstants() {
    }
}