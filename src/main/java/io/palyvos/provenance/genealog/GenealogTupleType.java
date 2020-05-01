package io.palyvos.provenance.genealog;

public enum GenealogTupleType {
  SOURCE,
  MAP,
  JOIN,
  AGGREGATE,
  REMOTE,
  /** Meta-tuple, which does not play any further part in the provenance traversals */
  META_SINK,
  META_SOURCE;
}
