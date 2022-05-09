package io.palyvos.provenance.ananke.aggregate;

import io.palyvos.provenance.genealog.GenealogTuple;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

class PointerListIterator implements Iterator<GenealogTuple> {

  private final GenealogTuple end;
  private GenealogTuple current;
  private boolean hasNext;

  public PointerListIterator(GenealogTuple start, GenealogTuple end) {
    this.current = start;
    this.end = end;
    this.hasNext = true;
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public GenealogTuple next() {
    if (!hasNext) {
      throw new NoSuchElementException("Iteration finished!");
    }
    final GenealogTuple result = current;
    if (Objects.equals(current, end)) {
      hasNext = false;
    } else {
      if (current == null) {
        throw new IllegalStateException(String.format(
            "Looks like provenance pointers are not ordered correctly. Try to use an order-independent aggregate strategy instead, e.g., %s",
            SortedPointersAggregateStrategy.class));
      }
      current = current.getNext();
    }
    return result;
  }
}
