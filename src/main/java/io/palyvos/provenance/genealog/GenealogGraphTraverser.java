package io.palyvos.provenance.genealog;

import io.palyvos.provenance.util.TimestampedUIDTuple;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GenealogGraphTraverser implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(GenealogGraphTraverser.class);
  private final ProvenanceAggregateStrategy aggregateStrategy;

  public GenealogGraphTraverser(ProvenanceAggregateStrategy aggregateStrategy) {
    this.aggregateStrategy = aggregateStrategy;
  }

  private static GenealogTuple earliest(GenealogTuple t1, GenealogTuple t2) {
    return t1.getTimestamp() < t2.getTimestamp() ? t1 : t2;
  }

  private static GenealogTuple latest(GenealogTuple t1, GenealogTuple t2) {
    return t1.getTimestamp() > t2.getTimestamp() ? t1 : t2;
  }

  public Pair<GenealogTuple, GenealogTuple> merge(
      GenealogTuple startA, GenealogTuple endA, GenealogTuple startB, GenealogTuple endB) {
    Optional<Pair<GenealogTuple, GenealogTuple>> fastMerge = fastMerge(startA, endA, startB, endB);
    if (fastMerge.isPresent()) {
      LOG.info("Merged Genealog windows using fast merge");
      return fastMerge.get();
    }
    LOG.info("Merging Genealog windows using slow merge");
    GenealogTuple iterA = startA;
    GenealogTuple iterB = startB;
    final GenealogTuple start;
    if (iterA.getTimestamp() < iterB.getTimestamp()) {
      start = iterA;
      iterA = getNext(iterA, endA);
    } else {
      start = iterB;
      iterB = getNext(iterB, endB);
    }
    GenealogTuple iter = start;
    while (iterA != null || iterB != null) {
      if (iterB == null || (iterA != null && iterA.getTimestamp() <= iterB.getTimestamp())) {
        iter.setNext(iterA);
        iter = iterA;
        iterA = getNext(iterA, endA);
      } else if (iterA == null || (iterB != null && iterB.getTimestamp() < iterA.getTimestamp())) {
        iter.setNext(iterB);
        iter = iterB;
        iterB = getNext(iterB, endB);
      } else {
        throw new IllegalStateException("Merge of GeneaLog windows failed!");
      }
    }
    return Pair.of(start, iter);
  }

  private Optional<Pair<GenealogTuple, GenealogTuple>> fastMerge(
      GenealogTuple startA, GenealogTuple endA, GenealogTuple startB, GenealogTuple endB) {
    GenealogTuple latestStart = latest(startA, startB);
    GenealogTuple earliestEnd = earliest(endA, endB);
    if (latestStart.getTimestamp() >= earliestEnd.getTimestamp()) {
      earliestEnd.setNext(latestStart);
      return Optional.of(Pair.of(earliest(startA, startB), latest(endA, endB)));
    }
    return Optional.empty();
  }

  private GenealogTuple getNext(GenealogTuple t, GenealogTuple end) {
    return t != end ? t.getNext() : null;
  }

  public Set<TimestampedUIDTuple> getProvenance(GenealogTuple tuple) {
    return traverseGraph(tuple);
  }

  public Set<TimestampedUIDTuple> getProvenance(GenealogData gdata) {
    return traverseGraph(new GenealogDataTupleAdapter(gdata));
  }

  private Set<TimestampedUIDTuple> traverseGraph(GenealogTuple start) {
    Set<TimestampedUIDTuple> result = new HashSet<>();
    Set<GenealogTuple> visited = new HashSet<>();
    ArrayDeque<GenealogTuple> queue = new ArrayDeque<>();
    queue.addLast(start);
    while (!queue.isEmpty()) {
      GenealogTuple t = queue.removeFirst();
      switch (t.getTupleType()) {
        case SOURCE:
          result.add(t);
          break;
        case MAP:
          addLastIfNotVisited(t.getU1(), queue, visited);
          break;
        case JOIN:
          addLastIfNotVisited(t.getU1(), queue, visited);
          addLastIfNotVisited(t.getU2(), queue, visited);
          break;
        case AGGREGATE:
          Iterator<GenealogTuple> it = aggregateStrategy.provenanceIterator(t);
          while (it.hasNext()) {
            addLastIfNotVisited(it.next(), queue, visited);
          }
          break;
        case REMOTE:
          result.addAll(t.getProvenance());
          break;
        case META_SOURCE:
          result.add(t.getSourceTuple());
          break;
        default:
          throw new IllegalStateException("Invalid TupleType: " + t.getTupleType());
      }
    }
    return result;
  }

  private void addLastIfNotVisited(
      GenealogTuple t, ArrayDeque<GenealogTuple> queue, Set<GenealogTuple> visited) {
    if (t == null) {
      throw new IllegalArgumentException("Null pointer in provenance graph!");
    }
    if (!visited.contains(t)) {
      visited.add(t);
      queue.addLast(t);
    }
  }
}
