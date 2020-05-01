package io.palyvos.provenance.util;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Assign a UID to a tuple using an {@link IncreasingUIDGenerator} and return the same tuple and
 * return that tuple at the output. Note that the same object is returned. This is usually safe for
 * provenance purposes, because Flink will not keep operators from different forks on the same task,
 * and thus there is (probably) no chance of concurrent modification of UIDs. Otherwise, it would be
 * safer to copy the tuple first.
 *
 * @param <T> The type of tuple.
 */
public class UIDAssigner<T extends UIDTuple> extends RichMapFunction<T, T> {

  private transient IncreasingUIDGenerator uidGenerator;
  private final int maxInstances;
  private final int componentIndex;

  public UIDAssigner(int componentIndex, int maxInstances) {
    this.componentIndex = componentIndex;
    this.maxInstances = maxInstances;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    Validate.isTrue(maxInstances >= getRuntimeContext().getMaxNumberOfParallelSubtasks(), "maxInstances > env.maxParallelism");
    this.uidGenerator =
        subtaskUIDGenerator(
            getRuntimeContext().getIndexOfThisSubtask(), componentIndex, maxInstances);
  }

  @Override
  public T map(T tuple) throws Exception {
    Validate.isTrue(tuple.getUID() == 0, "ID already set!");
    tuple.setUID(uidGenerator.newUID());
    return tuple;
  }
  /**
   * Create an increasing UID generator for a type of components, with some basic sanity checks to
   * avoid overlapping UIDs. Every component that wants to use the generator provides its subtask
   * index (as given by flink) and also its component index which is common for all subtasks. Then
   * the index of the generator is set equal to componentIndex + subtaskIndex.
   *
   * @param subtaskIndex The index of the subtask
   * @param componentIndex The index of the component, common between all subtasks
   * @param maxComponentInstances The maximum number of instances of this component
   * @return A new instance of an IncreasingUIDGenerator.
   */
  IncreasingUIDGenerator subtaskUIDGenerator(
      int subtaskIndex, int componentIndex, int maxComponentInstances) {
    Validate.isTrue(
        subtaskIndex <= maxComponentInstances, "MaxParallelism > maxComponentInstances");
    return new IncreasingUIDGenerator(componentIndex + subtaskIndex);
  }
}
