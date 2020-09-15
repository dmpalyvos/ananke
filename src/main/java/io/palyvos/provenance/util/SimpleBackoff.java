package io.palyvos.provenance.util;

import java.util.concurrent.ThreadLocalRandom;

public class SimpleBackoff {

  private static final int STEP_MILLIS = 20;
  private static final int MIN_SLEEP_MILLIS = 20;
  public static final int MAX_SLEEP_MILLIS = 100;
  private static final int MAX_RETRIES = MAX_SLEEP_MILLIS / STEP_MILLIS;
  private int retries = 0;

  public void backoff() {
    retries = Math.min(MAX_RETRIES, 1 + retries);
    final long sleep = ThreadLocalRandom.current()
        .nextInt(MIN_SLEEP_MILLIS, 1 + STEP_MILLIS * retries);
    try {
      Thread.sleep(sleep);
    } catch (InterruptedException e) {
      return;
    }
  }

  public void reset() {
    retries = 0;
  }
}
