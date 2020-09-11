package io.palyvos.provenance.util;

import org.apache.commons.lang3.Validate;

class TransactionalCountStat {

  private final CountStat stat;
  private long count = 0;

  public TransactionalCountStat(CountStat stat) {
    Validate.notNull(stat);
    this.stat = stat;
  }

  public void add(long v) {
    count += v;
  }

  public void commit() {
    stat.increase(count);
    reset();
  }

  public void reset() {
    count = 0;
  }

  public void close() {
    stat.close();
  }
}
