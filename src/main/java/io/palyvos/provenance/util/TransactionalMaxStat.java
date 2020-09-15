package io.palyvos.provenance.util;

import org.apache.commons.lang3.Validate;

class TransactionalMaxStat {

  public static final int DEFAULT_MAX_VALUE = -1;
  private final MaxStat stat;
  private long max = DEFAULT_MAX_VALUE;

  public TransactionalMaxStat(MaxStat stat) {
    Validate.notNull(stat);
    this.stat = stat;
  }

  public void add(long v) {
    max = Math.max(max, v);
  }

  public void commit() {
    stat.add(max);
    reset();
  }

  public void reset() {
    max = DEFAULT_MAX_VALUE;
  }

  public void close() {
    stat.close();
  }
}
