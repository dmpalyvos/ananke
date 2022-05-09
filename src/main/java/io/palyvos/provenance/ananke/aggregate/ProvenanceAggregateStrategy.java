package io.palyvos.provenance.ananke.aggregate;

import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.util.Accumulator;
import io.palyvos.provenance.genealog.GenealogTupleType;
import java.io.Serializable;
import java.util.Iterator;

/**
 * The interface that needs to be implemented by objects that handles provenance for aggregate
 * operators. Every {@link Accumulator} needs to have its own
 * {@link ProvenanceAggregateStrategy object} when constructing provenance graphs using
 * {@link #addWindowProvenance(GenealogTuple)} and {@link #annotateWindowResult(GenealogTuple)}. The
 * traversal method {@link #provenanceIterator(GenealogTuple)} acts as a factory for
 * {@link Iterator}s and thus does not require distinct {@link ProvenanceAggregateStrategy} objects
 * for different traversals.
 */
public interface ProvenanceAggregateStrategy extends Serializable {

  /**
   * Add a tuple to the provenance of this aggregate window.
   *
   * @param in  The input tuple.
   * @param <T> The type of the tuple.
   */
  <T extends GenealogTuple> void addWindowProvenance(T in);

  /**
   * Annotate the aggregation result with the current provenance of this aggregate window.
   *
   * @param result The result tuple that will be annotated with the provenance information.
   * @param <T>    The type of the result tuple.
   */
  <T extends GenealogTuple> void annotateWindowResult(T result);

  /**
   * Construct a new iterator for the provenance of the given tuple. The input tuple must be the
   * result of an aggregate operation (i.e., have {@link GenealogTupleType} equal to
   * {@link GenealogTupleType#AGGREGATE}
   *
   * @param tuple The tuple to get a provenance operator for.
   * @param <T>   The type of the tuple.
   * @return An {@link Iterator} for the provenance of the input tuple.
   */
  <T extends GenealogTuple> Iterator<GenealogTuple> provenanceIterator(T tuple);

  /**
   * Merge this strategy with another (e.g., for session windows), returning the merged
   * ({@code this}) strategy as a result.
   *
   * @param other Another strategy of the same type.
   * @return {@code this}, a merged strategy.
   */
  ProvenanceAggregateStrategy merge(ProvenanceAggregateStrategy other);
}
