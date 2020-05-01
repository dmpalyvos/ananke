package io.palyvos.provenance.usecases.synthetic.provenance.queries;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.usecases.synthetic.provenance.SyntheticProvenanceSource;
import io.palyvos.provenance.usecases.synthetic.provenance.SyntheticSinkTuple;
import io.palyvos.provenance.usecases.synthetic.provenance.SyntheticSourceTuple;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogDataSerializer;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.util.TimestampConverter;
import io.palyvos.provenance.ananke.stdops.HelperProvenanceGraphTuple;
import io.palyvos.provenance.ananke.functions.CustomGenericSerializer;
import java.util.Arrays;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class SyntheticQuery {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final TimestampConverter timestampConverter = (ts) -> ts;
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setMaxParallelism(settings.maxParallelism());

    env.getConfig().setAutoWatermarkInterval(settings.getWatermarkInterval());

    final GenealogDataSerializer genealogDataSerializer =
        GenealogDataSerializer.newInstance(settings.aggregateStrategySupplier().get(),
            settings.statisticsFolder(), settings.graphTraversalStatistics());

    env.addDefaultKryoSerializer(GenealogData.class, genealogDataSerializer);
    env.addDefaultKryoSerializer(SyntheticSourceTuple.class,
        new SyntheticSourceTuple.KryoSerializer());

    final CustomGenericSerializer customGenericSerializer = new CustomGenericSerializer();
    customGenericSerializer.register(SyntheticSourceTuple.class,
        new SyntheticSourceTuple.KryoSerializer());

    env.addDefaultKryoSerializer(HelperProvenanceGraphTuple.class,
        new HelperProvenanceGraphTuple.KryoSerializer(customGenericSerializer));

    DataStream<? extends GenealogTuple> sourceStream = env
        .addSource(new SyntheticProvenanceSource<>(SyntheticSinkTuple.supplier(),
            SyntheticSourceTuple.supplier(settings.syntheticTupleSize()), settings))
        .setParallelism(settings.syntheticSourceParallelism()).returns(SyntheticSinkTuple.class)
        .setParallelism(settings.syntheticSourceParallelism())
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SyntheticSinkTuple>() {
          @Override
          public long extractAscendingTimestamp(SyntheticSinkTuple element) {
            return timestampConverter.apply(element.getTimestamp());
          }
        }).startNewChain();

    settings.genealogActivator().activate(Arrays.asList(sourceStream), Arrays.asList("MAIN"),
        settings, settings.syntheticDelay(), timestampConverter);
    env.execute("DummyOptimizedQuery");
  }
}
