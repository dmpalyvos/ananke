package io.palyvos.provenance.util;

import com.esotericsoftware.kryo.Serializer;
import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogDataSerializer;
import io.palyvos.provenance.ananke.stdops.HelperProvenanceGraphTuple;
import io.palyvos.provenance.ananke.functions.CustomGenericSerializer;
import io.palyvos.provenance.ananke.functions.ProvenanceTupleContainer;
import io.palyvos.provenance.ananke.functions.ProvenanceTupleContainer.KryoSerializer;
import java.io.Serializable;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public enum FlinkSerializerActivator {
  NOPROVENANCE {
    @Override
    public SerializerRegistry activate(StreamExecutionEnvironment env,
        ExperimentSettings settings) {
      return new SerializerRegistry(env, null);
    }
  },
  PROVENANCE_OPTIMIZED {
    @Override
    public SerializerRegistry activate(StreamExecutionEnvironment env,
        ExperimentSettings settings) {

      final GenealogDataSerializer genealogDataSerializer =
          GenealogDataSerializer.newInstance(
              settings.aggregateStrategySupplier().get(),
              settings.statisticsFolder(),
              settings.graphTraversalStatistics());
      env.addDefaultKryoSerializer(GenealogData.class, genealogDataSerializer);

      env.addDefaultKryoSerializer(
          HelperProvenanceGraphTuple.class, new HelperProvenanceGraphTuple.KryoSerializer());
      return new SerializerRegistry(env, genealogDataSerializer);
    }
  },
  PROVENANCE_TRANSPARENT {
    @Override
    public SerializerRegistry activate(StreamExecutionEnvironment env,
        ExperimentSettings settings) {
      final GenealogDataSerializer genealogDataSerializer =
          GenealogDataSerializer.newInstance(
              settings.aggregateStrategySupplier().get(),
              settings.statisticsFolder(),
              settings.graphTraversalStatistics());

      env.addDefaultKryoSerializer(GenealogData.class, genealogDataSerializer);

      final CustomGenericSerializer customGenericSerializer = new CustomGenericSerializer();

      final KryoSerializer tupleContainerSerializer =
          new KryoSerializer(genealogDataSerializer, customGenericSerializer);
      env.addDefaultKryoSerializer(ProvenanceTupleContainer.class, tupleContainerSerializer);

      env.addDefaultKryoSerializer(
          HelperProvenanceGraphTuple.class,
          new HelperProvenanceGraphTuple.KryoSerializer(customGenericSerializer));

      return new SerializerRegistry(customGenericSerializer, genealogDataSerializer);
    }
  };

  /**
   * Adapter that provides a uniform interface for registering serializers in both {@link
   * StreamExecutionEnvironment} and {@link CustomGenericSerializer}.
   */
  public static class SerializerRegistry {

    private final StreamExecutionEnvironment env;
    private final CustomGenericSerializer genericSerializer;
    private final GenealogDataSerializer genealogDataSerializer;

    public SerializerRegistry(StreamExecutionEnvironment env,
        GenealogDataSerializer genealogDataSerializer) {
      this.env = env;
      this.genericSerializer = null;
      this.genealogDataSerializer = genealogDataSerializer;
    }

    public SerializerRegistry(CustomGenericSerializer genericSerializer,
        GenealogDataSerializer genealogDataSerializer) {
      this.env = null;
      this.genericSerializer = genericSerializer;
      this.genealogDataSerializer = genealogDataSerializer;
    }

    public <T, S extends Serializer<T> & Serializable> SerializerRegistry register(Class<T> clazz,
        S serializer) {
      if (env == null) {
        genericSerializer.register(clazz, serializer);
      } else {
        env.addDefaultKryoSerializer(clazz, serializer);
      }
      return this;
    }

    public GenealogDataSerializer genealogDataSerializer() {
      return genealogDataSerializer;
    }
  }

  public abstract SerializerRegistry activate(StreamExecutionEnvironment env,
      ExperimentSettings settings);
}
