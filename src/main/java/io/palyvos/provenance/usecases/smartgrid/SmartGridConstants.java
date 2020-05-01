package io.palyvos.provenance.usecases.smartgrid;

import java.util.Arrays;
import java.util.List;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SmartGridConstants {

  public static final String ANOMALY_AGGR_STREAM_KEY = "aggregate";
  public static final String ANOMALY_JOIN_STREAM_KEY = "join";
  public static final List<String> ANOMALY_BOTH_MULTIPLEX_OUTPUTS =
      Arrays.asList(ANOMALY_AGGR_STREAM_KEY, ANOMALY_JOIN_STREAM_KEY);
  public static final List<String> ANOMALY_AGGREGATE_MULTIPLEX_OUTPUT =
      Arrays.asList(ANOMALY_AGGR_STREAM_KEY);
  public static final Time ANOMALY_AGGR_WINDOW_SIZE = Time.hours(24);
  public static final Time ANOMALY_AGGR_WINDOW_SLIDE = Time.hours(24);
  public static final Time ANOMALY_JOIN_WINDOW_SIZE = Time.hours(1);
  public static final long ANOMALY_LIMIT = 200;
  public static final Time BLACKOUT_AGGREGATE_WINDOW_SIZE = Time.hours(24);
  public static final Time BLACKOUT_AGGREGATE_WINDOW_SLIDE = Time.hours(24);
  public static final int BLACKOUT_MAX_COUNT = 7;

  private SmartGridConstants() {

  }

}
