package io.palyvos.provenance.ananke.stdops;

import io.palyvos.provenance.util.ProvenanceActivator;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import io.palyvos.provenance.ananke.util.FlinkTimestampCopier;
import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.util.TimestampConverter;
import io.palyvos.provenance.ananke.output.ProvenanceGraphNode;
import io.palyvos.provenance.ananke.output.ProvenanceGraphNodeType;
import io.palyvos.provenance.ananke.output.ProvenanceGraphSink;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class StandardOperatorGraphProvenance {

  public static <T extends GenealogTuple> void activate(List<DataStream<T>> sinkStreams,
      List<String> names, ExperimentSettings settings, long maxDelayMillis,
      TimestampConverter timestampConverter) {

    final GenealogGraphTraverser genealogGraphTraverser =
        new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());

    DataStream<GenealogTuple> mergedSinkStream =
        ProvenanceActivator.getMergedSinkStream(sinkStreams, names, settings);

    DataStream<HelperProvenanceGraphTuple> unravellerOutput = mergedSinkStream
        .flatMap(new RichFlatMapFunction<GenealogTuple, HelperProvenanceGraphTuple>() {

          private static final long serialVersionUID = 1L;

          @Override
          public void flatMap(GenealogTuple t, Collector<HelperProvenanceGraphTuple> out) {
            for (TimestampedUIDTuple s : genealogGraphTraverser.getProvenance(t)) {
              out.collect(
                  HelperProvenanceGraphTuple.newSourceVertex(s.getTimestamp(), s, s.getUID()));
              //Timestamp set by FlinkTimestampCopier afterwards
              out.collect(
                  HelperProvenanceGraphTuple.newEdge(-1, s.getUID(), t.getUID()));
            }
            t.stopSerializingProvenance();
            //Timestamp set by FlinkTimestampCopier afterwards
            out.collect(HelperProvenanceGraphTuple.newSinkVertex(-1, t, t.getUID()));

          }
        }).setParallelism(settings.sinkParallelism()).name("ProvenanceUnravel");

    DataStream<HelperProvenanceGraphTuple> edgesOnly =
        unravellerOutput.filter(t -> t.type.equals(ProvenanceGraphNodeType.EDGE))
            .setParallelism(unravellerOutput.getParallelism());

    edgesOnly = FlinkTimestampCopier.connectToStream(edgesOnly, HelperProvenanceGraphTuple.class)
        .setParallelism(edgesOnly.getParallelism());

    DataStream<HelperProvenanceGraphTuple> edgesOnlyAdvancedInTime =
        edgesOnly.keyBy(t -> t.getUID())
            .window(SlidingEventTimeWindows.of(Time.milliseconds(2 * maxDelayMillis + 1),
                Time.milliseconds(maxDelayMillis)))
            .process(new GiveFirstHelperProvenanceGraphTuple())
            .setParallelism(edgesOnly.getParallelism())
            .filter(t -> t.rightBoundary - t.timestamp > maxDelayMillis)
            .setParallelism(edgesOnly.getParallelism());

    DataStream<HelperProvenanceGraphTuple> sinksOnly =
        unravellerOutput.filter(t -> t.type.equals(ProvenanceGraphNodeType.SINK))
            .setParallelism(unravellerOutput.getParallelism());

    sinksOnly = FlinkTimestampCopier.connectToStream(sinksOnly, HelperProvenanceGraphTuple.class)
        .setParallelism(sinksOnly.getParallelism());

    DataStream<HelperProvenanceGraphTuple> sourcesOnly = forwardOnlyOnceProvenanceGraphTuple(
        unravellerOutput.filter(t -> t.type.equals(ProvenanceGraphNodeType.SOURCE))
            .setParallelism(unravellerOutput.getParallelism()), maxDelayMillis,
        0);

    DataStream<HelperProvenanceGraphTuple> acks =
        sinksOnly.union(sourcesOnly).keyBy(t -> t.getUID())
            .window(SlidingEventTimeWindows.of(Time.milliseconds(2 * maxDelayMillis + 2),
                Time.milliseconds(maxDelayMillis)))
            .process(new GiveLastFirstSourceOrSinkTupleAsAck())
            .setParallelism(sinksOnly.getParallelism())
            .filter(t -> t.rightBoundary - t.timestamp > maxDelayMillis + 1)
            .setParallelism(sinksOnly.getParallelism());

    DataStream<HelperProvenanceGraphTuple> merged_streams =
        sourcesOnly.union(sinksOnly).union(edgesOnlyAdvancedInTime).union(acks);

    DataStreamSink<ProvenanceGraphNode> streamSink = merged_streams.keyBy(t -> t.getUID())
        .window(SlidingEventTimeWindows.of(Time.milliseconds(1), Time.milliseconds(1)))
        .process(new LastMerger())
        .setParallelism(merged_streams.getParallelism())
        .name("LAST-MERGER")
        .addSink(new ProvenanceGraphSink("SINK", settings, timestampConverter))
        .setParallelism(merged_streams.getParallelism())
        .name("SINK");

    if (settings.disableSinkChaining()) {
      streamSink.disableChaining();
    }

  }

  private static DataStream<HelperProvenanceGraphTuple> forwardOnlyOnceProvenanceGraphTuple(
      DataStream<HelperProvenanceGraphTuple> s, long C, long aggOffset) {

    SingleOutputStreamOperator<HelperProvenanceGraphTuple> a1 = s.keyBy(t -> t.getUID())
        .window(TumblingEventTimeWindows.of(Time.milliseconds(2 * C), Time.milliseconds(aggOffset)))
        .process(new MergeSources())
        .setParallelism(s.getParallelism());

    SingleOutputStreamOperator<HelperProvenanceGraphTuple> a2 = s.keyBy(t -> t.getUID())
        .window(
            TumblingEventTimeWindows.of(Time.milliseconds(2 * C), Time.milliseconds(aggOffset + C)))
        .process(new MergeSources())
        .setParallelism(s.getParallelism());

    return a1.join(a2).where(t -> t.getUID()).equalTo(t -> t.getUID())
        .window(SlidingEventTimeWindows.of(Time.milliseconds(C + 1), Time.milliseconds(C),
            Time.milliseconds(-1 + aggOffset)))
        .with(
            new JoinFunction<HelperProvenanceGraphTuple, HelperProvenanceGraphTuple, HelperProvenanceGraphTuple>() {

              private static final long serialVersionUID = 1L;

              @Override
              public HelperProvenanceGraphTuple join(HelperProvenanceGraphTuple first,
                  HelperProvenanceGraphTuple second) throws Exception {

                if (first.getCount() == second.getCount()
                    && first.rightBoundary > second.rightBoundary) {
                  return new HelperProvenanceGraphTuple(first, true);
                }
                if (first.getCount() == second.getCount()
                    && first.rightBoundary < second.rightBoundary) {
                  return new HelperProvenanceGraphTuple(second, true);
                }
                if (first.getCount() >= second.getCount()
                    && first.rightBoundary > second.rightBoundary) {
                  return new HelperProvenanceGraphTuple(first, true);
                }
                if (second.getCount() > first.getCount()
                    && second.rightBoundary > first.rightBoundary) {
                  return new HelperProvenanceGraphTuple(second, true);
                }
                return new HelperProvenanceGraphTuple(first, false);
              }
            })
        .setParallelism(a1.getParallelism())
        .filter(new FilterFunction<HelperProvenanceGraphTuple>() {

          private static final long serialVersionUID = 1L;

          @Override
          public boolean filter(HelperProvenanceGraphTuple value) throws Exception {
            return value.valid;
          }
        }).setParallelism(a1.getParallelism());

  }

  static class GiveFirstHelperProvenanceGraphTuple extends
      ProcessWindowFunction<HelperProvenanceGraphTuple, HelperProvenanceGraphTuple, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String key, Context context, Iterable<HelperProvenanceGraphTuple> input,
        Collector<HelperProvenanceGraphTuple> out) {

      HelperProvenanceGraphTuple t = input.iterator().next();

      out.collect(new HelperProvenanceGraphTuple(t.timestamp, context.window().maxTimestamp(),
          t.type, t.provenanceTuple, t.uid1, t.uid2, 1, true));

    }
  }

  static class LastMerger extends
      ProcessWindowFunction<HelperProvenanceGraphTuple, ProvenanceGraphNode, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String key, Context context, Iterable<HelperProvenanceGraphTuple> input,
        Collector<ProvenanceGraphNode> out) {

      HelperProvenanceGraphTuple t = input.iterator().next();

      switch (t.type) {
        case SOURCE:
          out.collect(ProvenanceGraphNode.newSource(t.provenanceTuple));
          break;
        case SINK:
          out.collect(ProvenanceGraphNode.newSink(t.provenanceTuple));
          break;
        case EDGE:
          out.collect(ProvenanceGraphNode.newEdge(t.uid1, t.uid2));
          break;
        case ACK:
          out.collect(ProvenanceGraphNode.newACK(t.uid1));
          break;
        default:
          break;
      }

    }
  }

  static class MergeSources extends
      ProcessWindowFunction<HelperProvenanceGraphTuple, HelperProvenanceGraphTuple, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String key, Context context, Iterable<HelperProvenanceGraphTuple> input,
        Collector<HelperProvenanceGraphTuple> out) {

      Iterator<HelperProvenanceGraphTuple> it = input.iterator();
      HelperProvenanceGraphTuple t = it.next();
      long count = 1;
      while (it.hasNext()) {
        t = it.next();
        count++;
      }
      out.collect(new HelperProvenanceGraphTuple(t.timestamp, context.window().maxTimestamp(),
          ProvenanceGraphNodeType.SOURCE, t.provenanceTuple, t.uid1, t.uid2, count, true));

    }
  }

  static class GiveLastFirstSourceOrSinkTupleAsAck extends
      ProcessWindowFunction<HelperProvenanceGraphTuple, HelperProvenanceGraphTuple, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String key, Context context, Iterable<HelperProvenanceGraphTuple> input,
        Collector<HelperProvenanceGraphTuple> out) {

      HelperProvenanceGraphTuple t = input.iterator().next();

      out.collect(new HelperProvenanceGraphTuple(
          t.type == ProvenanceGraphNodeType.SOURCE ? t.rightBoundary : t.timestamp,
          context.window().maxTimestamp(), ProvenanceGraphNodeType.ACK, t.provenanceTuple, t.uid1,
          t.uid2, 1, true));

    }
  }

}
