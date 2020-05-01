package io.palyvos.provenance.util;

import java.io.Serializable;
import java.util.function.Function;

/**
 * {@link Function} that converts a timestamp to milliseconds.
 */
public interface TimestampConverter extends Function<Long, Long>, Serializable {

}
