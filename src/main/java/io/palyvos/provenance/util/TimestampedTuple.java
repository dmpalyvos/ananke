package io.palyvos.provenance.util;

public interface TimestampedTuple {

  long getTimestamp();

  void setTimestamp(long timestamp);

  long getStimulus();

  void setStimulus(long stimulus);
}
