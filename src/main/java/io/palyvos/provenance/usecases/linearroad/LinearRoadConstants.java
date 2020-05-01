package io.palyvos.provenance.usecases.linearroad;

import org.apache.flink.streaming.api.windowing.time.Time;

public class LinearRoadConstants {

  public static final Time STOPPED_VEHICLE_WINDOW_SIZE = Time.seconds(120);
  public static final Time STOPPED_VEHICLE_WINDOW_SLIDE = Time.seconds(30);

  public static final Time ACCIDENT_WINDOW_SIZE = Time.seconds(30);
  public static final Time ACCIDENT_WINDOW_SLIDE = Time.seconds(30);

  private LinearRoadConstants() {

  }

}
