package io.palyvos.provenance.ananke.aggregate;

import io.palyvos.provenance.genealog.GenealogTuple;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

class PointerListIterator implements Iterator<GenealogTuple> {

  private final GenealogTuple start;
  private final GenealogTuple end;
  private GenealogTuple current;
  private boolean hasNext;

  public PointerListIterator(GenealogTuple start, GenealogTuple end) {
    this.start = start;
    this.end = end;
    this.current = start;
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
        throw new IllegalStateException();
      }
//          current,
//          "Incorrect provenance ordering. Please use an order-independent aggregate strategy instead. Start=%s, End=%s", start, end);
      current = current.getNext();
    }
    return result;
  }
}
