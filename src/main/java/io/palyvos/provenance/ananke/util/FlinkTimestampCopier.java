package io.palyvos.provenance.ananke.util;

import io.palyvos.provenance.util.TimestampedTuple;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * {@link OneInputStreamOperator} that copies the timestamps of each {@link
 * org.apache.flink.streaming.runtime.streamrecord.StreamElement} into the {@link
 * TimestampedTuple#getTimestamp()} attribute of the tuples in the stream. Note that the resulting
 * timestamps are always in milliseconds.
 *
 * @param <IN> The type of the tuples in the stream
 */
public class FlinkTimestampCopier<IN extends TimestampedTuple> extends AbstractStreamOperator<IN>
    implements OneInputStreamOperator<IN, IN> {

  public FlinkTimestampCopier() {
    setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  public static <IN extends TimestampedTuple> SingleOutputStreamOperator<IN> connectToStream(
      DataStream<IN> stream, Class<IN> clazz) {
    return stream.transform(
        FlinkTimestampCopier.class.getSimpleName(),
        TypeInformation.of(clazz),
        new FlinkTimestampCopier<>());
  }

  @Override
  public void processElement(StreamRecord<IN> element) {
    StreamRecord<IN> newElement =
        element.replace(updateFlinkTimestamp(element.getValue(), element.getTimestamp()));
    output.collect(newElement);
  }

  private IN updateFlinkTimestamp(IN tuple, long timestamp) {
    tuple.setTimestamp(timestamp);
    return tuple;
  }
}
