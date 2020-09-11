package io.palyvos.provenance.genealog;

import static org.neo4j.driver.Values.parameters;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.TimestampConverter;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink that writes {@link GenealogTuple} stream (including provenance) to PostgreSQL and records
 * latency.
 */
public class GenealogNeo4jSink<T extends GenealogTuple> extends RichSinkFunction<T> implements
    Consumer<Watermark> {

  private static Logger LOG = LoggerFactory.getLogger(GenealogNeo4jSink.class);

  private static final String NEO4J_URL = "neo4j://127.0.0.1:7687";
  private static final String NEO4J_USER = "neo4j";
  private static final String NEO4j_PASSWORD = "ananke";

  protected final ExperimentSettings settings;
  private final GenealogGraphTraverser genealogGraphTraverser;
  private final String name;
  private final long delayConstantMillis;
  private final TimestampConverter timestampConverter;
  private transient Driver driver;
  private int taskIndex;
  private transient KryoSerializer<TimestampedUIDTuple> serializer;
  private transient DataOutputSerializer outputSerializer;
  private boolean initialized = false;

  public GenealogNeo4jSink(String name, long delayConstantMillis, ExperimentSettings settings,
      TimestampConverter timestampConverter) {
    Validate.notNull(settings, "settings");
    Validate.notBlank(name, "name");
    this.name = name;
    this.delayConstantMillis = delayConstantMillis;
    this.settings = settings;
    this.genealogGraphTraverser = new GenealogGraphTraverser(
        settings.aggregateStrategySupplier().get());
    this.timestampConverter = timestampConverter;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    taskIndex = getRuntimeContext().getIndexOfThisSubtask();
    driver = GraphDatabase.driver(NEO4J_URL, AuthTokens.basic(NEO4J_USER, NEO4j_PASSWORD));
    serializer = new KryoSerializer<>(TimestampedUIDTuple.class,
        getRuntimeContext().getExecutionConfig());
    outputSerializer = new DataOutputSerializer(AbstractSqlSink.WRITE_BUFFER_START_SIZE);
    synchronized (GenealogNeo4jSink.class) {
      // WARNING
      // Will not work for distributed sinks
      // To handle this case, there should be a retry logic for transactions
      // and slightly different table creation
      // but locking is dirty and easy for experimental purposes
      if (!initialized) {
        initDatabase();
        initialized = true;
      }
    }
    try (Session session = driver.session()) {
      Transaction tx = session.beginTransaction();
      createExpired(0, tx);
      tx.commit();
    }
    super.open(parameters);
  }

  private void initDatabase() {
    try (Session session = driver.session()) {
      tryExecute(session, tx -> tx.run("MATCH (n) DETACH DELETE n"));
      tryExecute(session, tx -> tx.run("DROP INDEX sink_uid"));
      tryExecute(session, tx -> tx.run("DROP INDEX source_uid"));
      tryExecute(session, tx -> tx.run("CREATE INDEX sink_uid FOR (s:SINK) on (s.uid)"));
      tryExecute(session, tx -> tx.run("CREATE INDEX source_uid FOR (s:SOURCE) on (s.uid)"));
    }
  }

  private void tryExecute(Session session, TransactionWork<Result> work) {
   try {
    session.writeTransaction(work);
   }
   catch (Exception exception) {
     LOG.warn("Error initialing neo4j DB (probably can be ignored: {}", exception.getMessage());
   }
  }

  @Override
  public void accept(Watermark watermark) {
    try (Session session = driver.session()) {
      Transaction tx = session.beginTransaction();
      createExpired(watermark.getTimestamp(), tx);
      tx.commit();
    } catch (Exception exception) {
      LOG.error(exception.getMessage(), exception);
    }
  }

  private Result createExpired(long timestamp, Transaction tx) {
    return tx.run("MERGE (wm:WATERMARK { name: $name, task: $task }) "
            + "ON CREATE SET wm.ts = $ts, wm.stimulus=$stimulus "
            + "ON MATCH SET wm.ts = $ts, wm.stimulus=$stimulus ",
        parameters("name", name,
            "task", taskIndex,
            "ts", timestamp - delayConstantMillis,
            "stimulus", System.currentTimeMillis()));
  }

  @Override
  public void invoke(T tuple, Context context) {
    Set<TimestampedUIDTuple> provenance = genealogGraphTraverser.getProvenance(tuple);
    try (Session session = driver.session()) {
      Transaction tx = session.beginTransaction();
      createSink(tuple, tx);
      for (TimestampedUIDTuple source : provenance) {
        createSource(source, tx);
        createProvenance(source, tuple, tx);
      }
      tx.commit();
    } catch (Exception exception) {
      LOG.error(exception.getMessage(), exception);
    }
  }

  private Result createSink(T tuple, Transaction tx) {
    return tx.run("MERGE (s:SINK { uid: $uid }) "
            + "ON CREATE SET s.ts = $ts, s.data = $data",
        parameters("uid", tuple.getUID(),
            "ts", tuple.getTimestamp(),
            "data", AbstractSqlSink.serialize(tuple, serializer, outputSerializer)));
  }

  private Result createSource(TimestampedUIDTuple tuple, Transaction tx) {
    return tx.run("MERGE (s:SOURCE { uid: $uid }) "
            + "ON CREATE SET s.ts = $ts, s.data = $data",
        parameters("uid", tuple.getUID(),
            "ts", tuple.getTimestamp(),
            "data", AbstractSqlSink.serialize(tuple, serializer, outputSerializer)));
  }

  private Result createProvenance(TimestampedUIDTuple source, T tuple, Transaction tx) {
    return tx.run("MATCH (sink:SINK {uid: $sinkUID}) " +
            "MATCH (source:SOURCE {uid: $sourceUID}) " +
            "CREATE (source)-[:PROVENANCE]->(sink)",
        parameters("sinkUID", tuple.getUID(), "sourceUID", source.getUID()));
  }
}
