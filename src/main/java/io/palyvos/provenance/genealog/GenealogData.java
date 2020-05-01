package io.palyvos.provenance.genealog;

import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class GenealogData implements Serializable {

  private long uid;
  private GenealogTuple U1;
  private GenealogTuple U2;
  private AtomicReference<GenealogTuple> next = new AtomicReference<>();
  private GenealogTupleType tupleType;
  private Collection<TimestampedUIDTuple> provenance;
  // Only used with ListAggregateStrategy and never serialized
  private transient Collection<GenealogTuple> aggregateListProvenance;

  public void init(GenealogTupleType type) {
    setTupleType(type);
  }

  public GenealogTuple getU1() {
    return U1;
  }

  public void setU1(GenealogTuple u1) {
    U1 = u1;
  }

  public GenealogTuple getU2() {
    return U2;
  }

  public void setU2(GenealogTuple u2) {
    U2 = u2;
  }

  public GenealogTuple getNext() {
    return next.get();
  }

  public void setNext(GenealogTuple next) {
    this.next.set(next);
  }

  public boolean trySetNext(GenealogTuple expect, GenealogTuple next) {
    return this.next.compareAndSet(expect, next);
  }

  public GenealogTupleType getTupleType() {
    return tupleType;
  }

  public void setTupleType(GenealogTupleType tupleType) {
    this.tupleType = tupleType;
  }

  public long getUID() {
    return uid;
  }

  public void setUID(long uid) {
    this.uid = uid;
  }

  public Collection<TimestampedUIDTuple> getProvenance() {
    return provenance;
  }

  public void setProvenance(Collection<TimestampedUIDTuple> provenance) {
    this.provenance = provenance;
  }

  public Collection<GenealogTuple> getAggregateListProvenance() {
    return aggregateListProvenance;
  }

  public void setAggregateListProvenance(
      Collection<GenealogTuple> aggregateListProvenance) {
    this.aggregateListProvenance = aggregateListProvenance;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GenealogData that = (GenealogData) o;
    return Objects.equals(uid, that.uid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uid, tupleType);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("uid", uid)
        .append("U1", U1)
        .append("U2", U2)
        .append("next", next)
        .append("tupleType", tupleType)
        .append("provenance", provenance)
        .toString();
  }
}
