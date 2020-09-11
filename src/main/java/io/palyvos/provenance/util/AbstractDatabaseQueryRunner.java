package io.palyvos.provenance.util;

import io.palyvos.provenance.ananke.output.NoOpProvenanceGraphEncoder;
import io.palyvos.provenance.ananke.output.ProvenanceGraphEncoder;
import io.palyvos.provenance.ananke.output.TimestampedFileProvenanceGraphEncoder;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDatabaseQueryRunner implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDatabaseQueryRunner.class);
  public static final int STIMULUS_INDEX = 3;
  public static final int DEFAULT_TIMESTAMP = -1;
  public static final long FINAL_SLEEP_MILLIS = 500;
  public static final int TASK_INDEX = 0;
  private static final int TIMESTAMP_INDEX = 2;
  public static volatile long reads = 0;
  protected final AvgStat provenanceReadTimeStatistic;
  protected final TransactionalCountStat provenanceReadsStatistic;
  protected final TransactionalMaxStat deliveryLatencyStatistic;
  protected final long pollFrequencyMillis;
  protected final TransactionalProvenanceGraphEncoder encoder;
  protected final AnankeSqlQueries queries;
  private volatile boolean enabled = true;
  private final KryoSerializer<TimestampedUIDTuple> serializer;
  private final boolean dbFlowControl;

  public AbstractDatabaseQueryRunner(String name, ExperimentSettings settings,
      ExecutionConfig executionConfig,
      boolean writeToFile, AnankeSqlQueries queries) {
    Validate.isTrue(settings.pollFrequencyMillis() >= TASK_INDEX, "negative poll frequency");
    this.provenanceReadTimeStatistic = new AvgStat(
        settings.provenanceReadTimeFile(TASK_INDEX, name),
        settings.autoFlush());
    this.provenanceReadsStatistic = new TransactionalCountStat(
        new CountStat(settings.provenanceReadsFile(0, name), settings.autoFlush()));
    this.pollFrequencyMillis = settings.pollFrequencyMillis();
    ProvenanceGraphEncoder delegateEncoder =
        writeToFile ? new TimestampedFileProvenanceGraphEncoder(
            settings.outputFile(TASK_INDEX, name), settings.autoFlush())
            : new NoOpProvenanceGraphEncoder();
    this.encoder = new TransactionalProvenanceGraphEncoder(delegateEncoder);
    this.queries = queries;
    this.dbFlowControl = settings.dbFlowControl();
    serializer = new KryoSerializer<>(TimestampedUIDTuple.class, executionConfig);
    MaxStat delegateStatistic = new MaxStat(settings.deliveryLatencyFile(TASK_INDEX, name),
        settings.autoFlush());
    this.deliveryLatencyStatistic = new TransactionalMaxStat(delegateStatistic);
    AckDeliveryLatencyHelper.setStatistic(delegateStatistic);
  }

  @Override
  public void run() {
    while (enabled) {
      if (!trySleep(pollFrequencyMillis)) {
        break;
      }
      fetchData();
    }
    trySleep(FINAL_SLEEP_MILLIS);
    fetchData();
    provenanceReadTimeStatistic.close();
    deliveryLatencyStatistic.close();
    encoder.close();
  }

  public void stop() {
    enabled = false;
  }

  protected void fetchData() {
    SimpleBackoff backoff = new SimpleBackoff();
    boolean success = false;
    final long start = System.currentTimeMillis();
    while (!success) {
      try (Connection con = getConnection(); SQLCloseable finish = con::rollback) {
        con.setAutoCommit(false);
        con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        provenanceReadsStatistic.reset();
        deliveryLatencyStatistic.reset();
        AckDeliveryLatencyHelper.reset();
        encoder.reset();
        doFetch(con);
        con.commit();
        AckDeliveryLatencyHelper.commit();
        provenanceReadTimeStatistic.add(System.currentTimeMillis() - start);
        provenanceReadsStatistic.commit();
        deliveryLatencyStatistic.commit();
        encoder.commit();
        success = true;
      } catch (SQLException exception) {
        LOG.warn(exception.getMessage());
        backoff.backoff();
      }
    }
  }

  protected void doFetch(Connection con) throws SQLException {
    queries.prepareDatabase(con);
    processResult(queries.sinkQuery(con),
        rs -> {
          encoder.sinkVertex(rs.getLong("id"), summarizeBytes(rs.getBytes("data")));
          encoder.ack(rs.getLong("id"), DEFAULT_TIMESTAMP);
        });
    processResult(queries.sourceQuery(con),
        rs -> encoder.sourceVertex(rs.getLong("id"), summarizeBytes(rs.getBytes("data"))));
    processResult(queries.edgeQuery(con),
        rs -> encoder.edge(rs.getLong("source"), rs.getLong("sink"), DEFAULT_TIMESTAMP));
    processResult(queries.ackQuery(con),
        rs -> {
          encoder.ack(rs.getLong("id"), DEFAULT_TIMESTAMP);
          AckDeliveryLatencyHelper.observeTimestamp(rs.getLong(TIMESTAMP_INDEX));
        });
    queries.updateSink(con);
    queries.updateSource(con);
    queries.updateEdge(con);
    queries.updateAck(con);
  }

  protected void processResult(ResultSet rs, CheckedSQLConsumer<ResultSet> action)
      throws SQLException {
    final long now = System.currentTimeMillis();
    while (rs.next()) {
      long stimulus = rs.getLong(STIMULUS_INDEX);
      // Default stimulus is NULL -> 0 for some tables, so we only consider positive stimuli
      if (stimulus > 0) {
        deliveryLatencyStatistic.add(now - stimulus);
      }
      provenanceReadsStatistic.add(1);
      if (dbFlowControl) {
        reads += 1;
      }
      action.accept(rs);
    }
    rs.close();
  }

  static boolean trySleep(long millis) {
    if (millis == TASK_INDEX) {
      return true;
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      return false;
    }
    return true;
  }

  String deserialize(byte[] bytes) {
    try {
      DataInputDeserializer input = new DataInputDeserializer(bytes);
      return serializer.deserialize(input).toString();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  static final String summarizeBytes(byte[] bytes) {
    return String.format("%s (%d bytes)", bytes.toString(), bytes.length);
  }

  protected abstract Connection getConnection() throws SQLException;
}
