package io.palyvos.provenance.ananke.output;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.TimestampConverter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class ProvenanceGraphSink extends RichSinkFunction<ProvenanceGraphNode> {

  private final ExperimentSettings settings;
  private final TimestampConverter timestampConverter;
  private transient ProvenanceGraphEncoder graphEncoder;
  private final String name;

  public ProvenanceGraphSink(String name, ExperimentSettings settings,
      TimestampConverter timestampConverter) {
    this.name = name;
    this.settings = settings;
    this.timestampConverter = timestampConverter;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.graphEncoder = settings.newGraphEncoder(name, getRuntimeContext().getIndexOfThisSubtask());
  }

  @Override
  public void invoke(ProvenanceGraphNode value, Context context) throws Exception {
    switch (value.type) {
      case SOURCE:
        graphEncoder.sourceVertex(value.sourceTuple(), context.timestamp(), timestampConverter.apply(value.sourceTuple().getTimestamp()));
        break;
      case SINK:
        graphEncoder.sinkVertex(value.sinkTuple(), context.timestamp(), timestampConverter.apply(value.sinkTuple().getTimestamp()));
        break;
      case EDGE:
        graphEncoder.edge(value.sourceUID(), value.sinkUID(), context.timestamp());
        break;
      case ACK:
        graphEncoder.ack(value.ackUID(), context.timestamp());
        break;
      default:
        throw new IllegalStateException(String.format("Unsupported ProvenanceGraphNodeType: %s", value.type));
    }
  }

  @Override
  public void close() throws Exception {
    graphEncoder.close();
    super.close();
  }
}
