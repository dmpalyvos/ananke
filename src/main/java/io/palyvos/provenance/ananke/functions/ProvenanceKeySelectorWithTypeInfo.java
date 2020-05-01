package io.palyvos.provenance.ananke.functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

public class ProvenanceKeySelectorWithTypeInfo<IN, KEY> extends
    ProvenanceKeySelector<IN, KEY> implements
    ResultTypeQueryable<KEY> {

  private final Class<KEY> clazz;

  public ProvenanceKeySelectorWithTypeInfo(
      KeySelector<IN, KEY> delegate, Class<KEY> clazz) {
    super(delegate);
    this.clazz = clazz;
  }

  @Override
  public TypeInformation<KEY> getProducedType() {
    return TypeInformation.of(clazz);
  }
}
