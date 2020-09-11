package io.palyvos.provenance.genealog;

import io.palyvos.provenance.util.AbstractDatabaseQueryRunner;
import io.palyvos.provenance.util.AckDeliveryLatencyHelper;
import io.palyvos.provenance.util.AvgStat;
import io.palyvos.provenance.util.CountStat;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.SQLCloseable;
import io.palyvos.provenance.util.SimpleBackoff;
import io.palyvos.provenance.util.TimestampConverter;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSqlSink<T extends GenealogTuple> extends
    RichSinkFunction<T> implements Consumer<Watermark> {

  private static final String INSERT_SOURCE = "INSERT INTO source(id, ts, data, stimulus) VALUES(?,?,?,?) ON CONFLICT DO NOTHING";
  private static final String INSERT_SINK = "INSERT INTO sink(id, ts, data, stimulus) VALUES(?,?,?,?) ON CONFLICT DO NOTHING";
  private static final String INSERT_EDGE = "INSERT INTO edge(source, sink, stimulus) VALUES(?,?,?) ON CONFLICT DO NOTHING";
  private static final String INSERT_EXPIRED = "INSERT INTO expired(id, task, ts) VALUES(?,?,?);";
  private static final String UPDATE_EXPIRED = "UPDATE expired SET ts = ? WHERE id = ? AND task = ?";
  public static final int WRITE_BUFFER_START_SIZE = 100;
  public static final AtomicLong WRITES = new AtomicLong(0);
  private static final long MAX_PENDING = 1000;
  public static final int FLOW_CONTROL_SLEEP_MILLIS = 100;
  private static Logger LOG = LoggerFactory.getLogger(AbstractSqlSink.class);
  protected final ExperimentSettings settings;
  protected final GenealogGraphTraverser genealogGraphTraverser;
  protected final String name;
  protected final long delayConstantMillis;
  protected final TimestampConverter timestampConverter;
  private transient KryoSerializer<TimestampedUIDTuple> serializer;
  private transient DataOutputSerializer serializationOutput;
  private transient CountStat provenanceWritesStatistic;
  private transient AvgStat provenanceWriteTimeStatistic;
  private int taskIndex;

  public AbstractSqlSink(
      ExperimentSettings settings, String name, long delayConstantMillis,
      TimestampConverter timestampConverter) {
    this.settings = settings;
    this.genealogGraphTraverser = new GenealogGraphTraverser(
        settings.aggregateStrategySupplier().get());
    this.name = name;
    this.delayConstantMillis = delayConstantMillis;
    this.timestampConverter = timestampConverter;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    taskIndex = getRuntimeContext().getIndexOfThisSubtask();
    serializer = new KryoSerializer<>(TimestampedUIDTuple.class,
        getRuntimeContext().getExecutionConfig());
    serializationOutput = new DataOutputSerializer(WRITE_BUFFER_START_SIZE);
    provenanceWritesStatistic = new CountStat(
        settings.provenanceWritesFile(getRuntimeContext().getIndexOfThisSubtask(), name),
        settings.autoFlush());
    provenanceWriteTimeStatistic = new AvgStat(settings
        .provenanceWriteTimeFile(taskIndex, name),
        settings.autoFlush());
    super.open(parameters);
  }

  @Override
  public void close() throws Exception {
    provenanceWritesStatistic.close();
    provenanceWriteTimeStatistic.close();
    super.close();
  }

  @Override
  public void invoke(T tuple, Context context) throws Exception {
    Set<TimestampedUIDTuple> provenance = genealogGraphTraverser.getProvenance(tuple);
    final long start = System.currentTimeMillis();
    boolean success = false;
    flowControl(provenance.size());
    SimpleBackoff backoff = new SimpleBackoff();
    while (!success) {
      try (Connection con = getConnection(); SQLCloseable finish = con::rollback) {
        con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        con.setAutoCommit(false);
        insertSink(tuple, con, start);
        insertSources(provenance, con, start);
        insertEdges(provenance, tuple, con, start);
        // NOTE: Writes will be more than reality for provenance overlap > 1
        // But this implementation is consistent with the DB implementations
        // "Wrote" 1 sink  + 1 sink ACK + P sources + P edges + P ACKs
        provenanceWritesStatistic.increase(2 + 3 * provenance.size());
        provenanceWriteTimeStatistic.add(System.currentTimeMillis() - start);
        con.commit();
        success = true;
        if (settings.dbFlowControl()) {
          WRITES.addAndGet(2 + 3 * provenance.size());
        }
      } catch (SQLException exception) {
        LOG.warn("Insert tuples {}", exception.getMessage());
        backoff.backoff();
      }
    }
  }

  private void flowControl(int provenanceSize) throws InterruptedException {
    while (settings.dbFlowControl() && (WRITES.get() - AbstractDatabaseQueryRunner.reads
        > MAX_PENDING * 3 * provenanceSize)) {
      Thread.sleep(FLOW_CONTROL_SLEEP_MILLIS);
    }
  }

  @Override
  public void accept(Watermark watermark) {
    final long start = System.currentTimeMillis();
    boolean success = false;
    SimpleBackoff backoff = new SimpleBackoff();
    while (!success) {
      try (Connection con = getConnection(); SQLCloseable finish = con::rollback) {
        con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        con.setAutoCommit(false);
        updateExpired(watermark.getTimestamp(), start, con);
        con.commit();
        success = true;
      } catch (SQLException exception) {
        LOG.warn("Write watermark: {}", exception.getMessage());
        backoff.backoff();
      }
    }
    provenanceWriteTimeStatistic.add(System.currentTimeMillis() - start);
  }

  protected abstract Connection getConnection() throws SQLException;

  protected void initExpired(Connection con) throws SQLException {
    try (PreparedStatement preparedStatement = con.prepareStatement(INSERT_EXPIRED)) {
      preparedStatement.setString(1, name);
      preparedStatement.setInt(2, taskIndex);
      preparedStatement.setLong(3, -delayConstantMillis);
      preparedStatement.execute();
    }
  }

  protected void updateExpired(long watermark, long stimulus, Connection con) throws SQLException {
    try (PreparedStatement preparedStatement = con.prepareStatement(UPDATE_EXPIRED)) {
      preparedStatement.setLong(1, watermark - delayConstantMillis);
      preparedStatement.setString(2, name);
      preparedStatement.setInt(3, taskIndex);
      preparedStatement.execute();
    }
    ResultSet rs = con.createStatement().executeQuery("SELECT MIN(ts) FROM expired");
    rs.next();
    AckDeliveryLatencyHelper.updateExpired(rs.getLong(1), stimulus);
  }

  protected void insertEdges(Set<TimestampedUIDTuple> sources, T sink, Connection con,
      long stimulus)
      throws SQLException {
    try (PreparedStatement preparedStatement = con.prepareStatement(INSERT_EDGE)) {
      for (TimestampedUIDTuple source : sources) {
        preparedStatement.setLong(1, source.getUID());
        preparedStatement.setLong(2, sink.getUID());
        preparedStatement.setLong(3, stimulus);
        preparedStatement.addBatch();
      }
      preparedStatement.executeBatch();
    }
  }

  protected void insertSources(Set<TimestampedUIDTuple> tuples, Connection con, long stimulus)
      throws SQLException {
    try (PreparedStatement preparedStatement = con.prepareStatement(INSERT_SOURCE)) {
      for (TimestampedUIDTuple source : tuples) {
        setVertexAttributes(preparedStatement, source, stimulus, con);
        preparedStatement.addBatch();
      }
      preparedStatement.executeBatch();
    }
  }

  protected void insertSink(T tuple, Connection con, long stimulus) throws SQLException {
    try (PreparedStatement preparedStatement = con.prepareStatement(INSERT_SINK)) {
      setVertexAttributes(preparedStatement, tuple, stimulus, con);
      preparedStatement.execute();
    }
  }

  protected void setVertexAttributes(PreparedStatement preparedStatement, TimestampedUIDTuple tuple,
      long stimulus, Connection connection) throws SQLException {
    preparedStatement.setLong(1, tuple.getUID());
    preparedStatement.setLong(2, timestampConverter.apply(tuple.getTimestamp()));
    preparedStatement.setBytes(3, serialize(tuple, serializer, serializationOutput));
    preparedStatement.setLong(4, stimulus);
  }

  static byte[] serialize(TimestampedUIDTuple tuple, KryoSerializer<TimestampedUIDTuple> serializer,
      DataOutputSerializer outputSerializer) {
    if (tuple instanceof GenealogTuple) {
      ((GenealogTuple) tuple).stopSerializingProvenance();
    }
    outputSerializer.clear();
    try {
      serializer.serialize(tuple, outputSerializer);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return outputSerializer.getCopyOfBuffer();
  }
}
