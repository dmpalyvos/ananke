package io.palyvos.provenance.util;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncreasingUIDGenerator {
  private static Logger LOG = LoggerFactory.getLogger(IncreasingUIDGenerator.class);

  public static final int AVAILABLE_BITS = 64;
  public static final int GENERATOR_BITS = 6;
  public static final int UID_BITS = AVAILABLE_BITS - GENERATOR_BITS;
  public static final long GENERATOR_ID_LIMIT = 1L << GENERATOR_BITS;
  public static final long UID_LIMIT = 1L << UID_BITS;
  private static final long UID_MASK = UID_LIMIT - 1L;

  private final long generatorId;
  private long uidCounter = 0;



  public IncreasingUIDGenerator(long generatorId) {
    Validate.isTrue(generatorId < GENERATOR_ID_LIMIT);
    LOG.info("Initializing {} with index {}", getClass().getSimpleName(), generatorId);
    this.generatorId = generatorId;
  }

  /**
   * Generate a new UUID. The leftmost {@link #GENERATOR_BITS} are reserved for the generator ID
   * (which is needs to be unique) and the rest of the bits (i.e., the rightmost {@link #UID_BITS}
   * are reserved for an increasing counter, local to this generator. the counter can (and will)
   * overflow, so the UIDs are actually unique only if there are less than 2^{@link #UID_BITS} of
   * them in the system, for every generator.
   *
   * @return A new unique ID for this generator (or a recycled ID after 2^{@link #UID_BITS} IDs have
   *     passed.
   */
  public long newUID() {
    return (generatorId << UID_BITS) | (uidCounter++ & UID_MASK);
  }

  public static String asString(long uid) {
    long generatorIndex = (uid & ~UID_MASK) >>> UID_BITS;
    long index = uid & UID_MASK;
//    return String.format("%s:%s", Long.toHexString(generatorIndex), Long.toHexString(index));
    return Long.toString(uid);
  }
}
