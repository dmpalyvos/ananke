package io.palyvos.provenance.usecases.cars.cloud;

import org.apache.flink.streaming.api.windowing.time.Time;

public class CarCloudConstants {
    public static final Time INSTANT_SPEED_WINDOW_SIZE = Time.seconds(15);
    public static final Time INSTANT_SPEED_WINDOW_ADVANCE = Time.seconds(5);
    public static final Time AVERAGE_SPEED_WINDOW_SIZE = Time.seconds(120);
    public static final Time AVERAGE_SPEED_WINDOW_ADVANCE = Time.seconds(60);
    public static final int AVERAGE_SPEED_MINIMUM = 70;

    public static final double UPPER_LATITUDE = 39.9206835;
    public static final double LOWER_LATITUDE = 39.912915;
    public static final double UPPER_LONGITUDE = 116.3188575;
    public static final double LOWER_LONGITUDE = 116.299221;
    public static final Time TIME_AREA_WINDOW_SIZE = Time.seconds(300);
    public static final Time TIME_AREA_WINDOW_ADVANCE = Time.seconds(150);
    public static final int AREA_MIN_COUNT = 9;

    public static final double LAT_CONVERSION = 111132.954 - 559.822 * 1 + 1.175 * 1;
    public static final double LON_CONVERSION = 111132.954 * 1;
}
