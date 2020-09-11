package io.palyvos.provenance.util;

import io.palyvos.provenance.ananke.output.ProvenanceGraphExtractor;
import io.palyvos.provenance.ananke.output.ProvenanceGraphNode;
import io.palyvos.provenance.ananke.output.ProvenanceGraphSink;
import io.palyvos.provenance.ananke.stdops.StandardOperatorGraphProvenance;
import io.palyvos.provenance.ananke.util.WatermarkTriggeringFlatMap;
import io.palyvos.provenance.genealog.GenealogFileSink;
import io.palyvos.provenance.genealog.GenealogMongoSink;
import io.palyvos.provenance.genealog.GenealogNeo4jSink;
import io.palyvos.provenance.genealog.GenealogPostgresSink;
import io.palyvos.provenance.genealog.GenealogSqliteSink;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.WatermarkTriggeringSink;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public enum ProvenanceActivator {
  GENEALOG {
    @Override
    public <T extends GenealogTuple> void activate(
        List<DataStream<T>> sinkStreams,
        List<String> names,
        ExperimentSettings settings,
        long maxDelayMillis,
        TimestampConverter timestampConverter) {
      Validate.isTrue(sinkStreams.size() == names.size(), "streams.size() != names.size()");
      for (int i = 0; i < sinkStreams.size(); i++) {
        DataStream<T> stream = sinkStreams.get(i);
        String name = names.get(i);
        stream
            .addSink(GenealogFileSink.newInstance(name, settings))
            .setParallelism(settings.sinkParallelism())
            .name(name);
      }
    }

    @Override
    public <T extends UIDTuple> MapFunction<T, T> uidAssigner(
        int componentIndex, int maxInstances) {
      return (MapFunction<T, T>) value -> value;
    }
  },
  ANANKE_POSTGRES {
    @Override
    public <T extends GenealogTuple> void activate(
        List<DataStream<T>> sinkStreams,
        List<String> names,
        ExperimentSettings settings,
        long maxDelayMillis,
        TimestampConverter timestampConverter) {
      genealogToExternalSink(
          (name) -> new GenealogPostgresSink<>(name, maxDelayMillis,
              settings, timestampConverter), sinkStreams, names,
          settings);
    }

    @Override
    public <T extends UIDTuple> MapFunction<T, T> uidAssigner(
        int componentIndex, int maxInstances) {
      return new UIDAssigner<>(componentIndex, maxInstances);
    }
  },
  ANANKE_SQLITE {
    @Override
    public <T extends GenealogTuple> void activate(
        List<DataStream<T>> sinkStreams,
        List<String> names,
        ExperimentSettings settings,
        long maxDelayMillis,
        TimestampConverter timestampConverter) {
      genealogToExternalSink(
          (name) -> new GenealogSqliteSink<>(name, maxDelayMillis,
              settings, timestampConverter), sinkStreams, names,
          settings);
    }

    @Override
    public <T extends UIDTuple> MapFunction<T, T> uidAssigner(
        int componentIndex, int maxInstances) {
      return new UIDAssigner<>(componentIndex, maxInstances);
    }
  },
  ANANKE_NEO4J {
    @Override
    public <T extends GenealogTuple> void activate(
        List<DataStream<T>> sinkStreams,
        List<String> names,
        ExperimentSettings settings,
        long maxDelayMillis,
        TimestampConverter timestampConverter) {
      genealogToExternalSink((name) -> new GenealogNeo4jSink<>(name,
          maxDelayMillis, settings, timestampConverter), sinkStreams, names, settings);
    }

    @Override
    public <T extends UIDTuple> MapFunction<T, T> uidAssigner(
        int componentIndex, int maxInstances) {
      return new UIDAssigner<>(componentIndex, maxInstances);
    }
  },
  ANANKE_MONGO {
    @Override
    public <T extends GenealogTuple> void activate(
        List<DataStream<T>> sinkStreams,
        List<String> names,
        ExperimentSettings settings,
        long maxDelayMillis,
        TimestampConverter timestampConverter) {
      genealogToExternalSink((name) -> new GenealogMongoSink<>(name,
          maxDelayMillis, settings, timestampConverter), sinkStreams, names, settings);
    }

    @Override
    public <T extends UIDTuple> MapFunction<T, T> uidAssigner(
        int componentIndex, int maxInstances) {
      return new UIDAssigner<>(componentIndex, maxInstances);
    }
  },
  ANANKE {
    @Override
    public <T extends GenealogTuple> void activate(
        List<DataStream<T>> sinkStreams,
        List<String> names,
        ExperimentSettings settings,
        long maxDelayMillis,
        TimestampConverter timestampConverter) {

      Validate.isTrue(sinkStreams.size() == names.size(), "streams.size() != names.size()");
      ProvenanceGraphExtractor<GenealogTuple> graphExtractor =
          new ProvenanceGraphExtractor<>(settings, maxDelayMillis, timestampConverter);
      DataStream<GenealogTuple> mergedSinkStream =
          getMergedSinkStream(sinkStreams, names, settings);
      DataStream<ProvenanceGraphNode> graphNodeDataStream = WatermarkTriggeringFlatMap.connect(
          mergedSinkStream, graphExtractor,
          graphExtractor.watermarkConsumer()
      ).setParallelism(1).name(name());
      DataStreamSink<ProvenanceGraphNode> streamSink = graphNodeDataStream
          .addSink(new ProvenanceGraphSink("SINK", settings, timestampConverter))
          .setParallelism(1)
          .slotSharingGroup(settings.thirdSlotSharingGroup());
      if (settings.disableSinkChaining()) {
        streamSink.disableChaining();
      }
    }

    @Override
    public <T extends UIDTuple> MapFunction<T, T> uidAssigner(
        int componentIndex, int maxInstances) {
      return new UIDAssigner<>(componentIndex, maxInstances);
    }
  },
  ANANKE_STD {
    @Override
    public <T extends GenealogTuple> void activate(
        List<DataStream<T>> sinkStreams,
        List<String> names,
        ExperimentSettings settings,
        long maxDelayMillis,
        TimestampConverter timestampConverter) {
      StandardOperatorGraphProvenance.activate(
          sinkStreams, names, settings, maxDelayMillis, timestampConverter);
    }

    @Override
    public <T extends UIDTuple> MapFunction<T, T> uidAssigner(
        int componentIndex, int maxInstances) {
      return new UIDAssigner<>(componentIndex, maxInstances);
    }
  };

  protected <T extends GenealogTuple, S extends SinkFunction<GenealogTuple> & Consumer<Watermark>> void genealogToExternalSink(
      Function<String, S> sinkSupplier,
      List<DataStream<T>> sinkStreams,
      List<String> names, ExperimentSettings settings) {
    Validate.isTrue(sinkStreams.size() == names.size(), "streams.size() != names.size()");
    for (int i = 0; i < sinkStreams.size(); i++) {
      int componentIndex = (settings.sourcesNumber() + i) * settings.maxParallelism();
      DataStream<T> sinkStream = sinkStreams.get(i);
      String name = names.get(i);
      DataStream<GenealogTuple> streamSink =
          sinkStream
              .map(new LatencyLoggingMap<T, GenealogTuple>(names.get(i), settings))
              .name("LatencyLogger")
              .returns(GenealogTuple.class)
              .map(new UIDAssigner<>(componentIndex, settings.maxParallelism()))
              .setParallelism(sinkStream.getParallelism())
              .name("UIDAssigner");
      S sinkFunction = sinkSupplier.apply(name);
      WatermarkTriggeringSink.addSink(streamSink, name, settings.sinkParallelism(),
          settings.disableSinkChaining(), sinkFunction, sinkFunction);
    }
  }

  public static <T extends GenealogTuple> DataStream<GenealogTuple> getMergedSinkStream(
      List<DataStream<T>> sinkStreams, List<String> names, ExperimentSettings settings) {
    List<DataStream<GenealogTuple>> extendedStreams = new ArrayList<>();
    for (int i = 0; i < sinkStreams.size(); i++) {
      int componentIndex = (settings.sourcesNumber() + i) * settings.maxParallelism();
      DataStream<T> sinkStream = sinkStreams.get(i);
      DataStream<GenealogTuple> extendedStream =
          sinkStream
              .map(new LatencyLoggingMap<T, GenealogTuple>(names.get(i), settings))
              .setParallelism(sinkStream.getParallelism())
              .name("LatencyLogger")
              .returns(GenealogTuple.class)
              .map(new UIDAssigner<>(componentIndex, settings.maxParallelism()))
              .setParallelism(sinkStream.getParallelism())
              .name("UIDAssigner");
      extendedStreams.add(extendedStream);
    }
    DataStream<GenealogTuple> union = extendedStreams.get(0);
    if (extendedStreams.size() > 1) {
      union =
          union.union(
              extendedStreams.subList(1, extendedStreams.size()).toArray(new DataStream[0]));
    }
    return union;
  }

  public abstract <T extends GenealogTuple> void activate(
      List<DataStream<T>> sinkStreams,
      List<String> names,
      ExperimentSettings settings,
      long maxDelayMillis,
      TimestampConverter timestampConverter);

  public static <T extends GenealogTuple> DataStream<GenealogTuple> convert(
      DataStream<T> sinkStream) {
    return (DataStream<GenealogTuple>) sinkStream;
  }

  public abstract <T extends UIDTuple> MapFunction<T, T> uidAssigner(
      int componentIndex, int maxInstances);
}
