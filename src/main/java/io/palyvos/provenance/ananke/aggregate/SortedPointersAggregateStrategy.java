package io.palyvos.provenance.ananke.aggregate;

import io.palyvos.provenance.genealog.GenealogTuple;

/**
 * {@link ProvenanceAggregateStrategy} that maintains provenance as a sorted pointer list. It is able
 * to handle out-of-order arrivals inside a window.
 */
public class SortedPointersAggregateStrategy extends UnsortedPointersAggregateStrategy {

  @Override
  public <T extends GenealogTuple> void doInsert(T in) {
    // Fast path: in.ts < earliest.ts or in.ts > latest.ts
    if (insertAtBeginningOrEnd(in)) {
      return;
    }
    GenealogTuple current = earliest;
    while (true) {
      GenealogTuple next = current.getNext();
      if (current.equals(in)) { // already inserted
        return;
      }
      if (tryInsert(in, current, next)) {
        return;
      }
      current = next;
    }
  }

  private <T extends GenealogTuple> boolean insertAtBeginningOrEnd(T in) {
    if (earliest.getTimestamp() > in.getTimestamp()) {
      in.setNext(earliest);
      earliest = in;
      return true;
    } else if (latest.getTimestamp() < in.getTimestamp()) {
      latest.setNext(in);
      latest = in;
      return true;
    }
    return false;
  }

  private <T extends GenealogTuple> boolean tryInsert(
      T in, GenealogTuple current, GenealogTuple next) {
    final boolean isEndOfWindow = current.equals(latest);
    final boolean insertHere =
        isEndOfWindow || next == null || next.getTimestamp() > in.getTimestamp();
    if (!insertHere) {
      return false;
    }
    in.setNext(next);
    current.setNext(in);
    if (isEndOfWindow) {
      latest = in;
    }
    return true;
  }
}
