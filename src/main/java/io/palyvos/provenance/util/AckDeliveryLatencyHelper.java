package io.palyvos.provenance.util;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Helper for recording delivery latency statistics for the acks with a low overhead. There can be
 * many threads concurrently calling {@link #updateExpired(long, long)} but there can only be one
 * thread calling {@link #observeTimestamp(long)}, {@link #commit()} and {@link #reset()}.
 */
public class AckDeliveryLatencyHelper {

  private static final long DEFAULT_MIN_TIMESTAMP = Long.MAX_VALUE;
  private static final int DEFAULT_MAX_TIMESTAMP = -1;
  private static final ConcurrentSkipListMap<Long, Long> EXPIRED_LOG = new ConcurrentSkipListMap<>();
  private static final int EXPIRED_LOG_UPDATE_PERIOD = 10;
  private static int watermarkPollCount = 0;
  private static MaxStat deliveryLatencyStatistic;
  private static long minTimestamp = DEFAULT_MIN_TIMESTAMP;
  private static long maxTimestamp = DEFAULT_MAX_TIMESTAMP;

  static void setStatistic(MaxStat statistic) {
    deliveryLatencyStatistic = statistic;
  }

  public static void observeTimestamp(long timestamp) {
    minTimestamp = timestamp < minTimestamp ? timestamp : minTimestamp;
    maxTimestamp = timestamp > maxTimestamp ? timestamp : maxTimestamp;
  }

  public static void updateExpired(long timestamp, long stimulus) {
    EXPIRED_LOG.putIfAbsent(timestamp, stimulus);
  }

  public static void commit() {
    if (maxTimestamp < 0) {
      // No record was processed
      return;
    }
    Map.Entry<Long, Long> entry = EXPIRED_LOG.ceilingEntry(minTimestamp + 1);
    if (entry == null) {
      return;
    }
    deliveryLatencyStatistic.add(System.currentTimeMillis() - entry.getValue());
    watermarkPollCount = (watermarkPollCount + 1) % EXPIRED_LOG_UPDATE_PERIOD;
    if (watermarkPollCount == 0) {
      EXPIRED_LOG.subMap(0L, maxTimestamp + 1).clear();
    }
    reset();
  }

  public static void reset() {
    minTimestamp = DEFAULT_MIN_TIMESTAMP;
    maxTimestamp = DEFAULT_MAX_TIMESTAMP;
  }

  private AckDeliveryLatencyHelper() {

  }

}
