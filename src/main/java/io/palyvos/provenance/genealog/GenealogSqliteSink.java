package io.palyvos.provenance.genealog;

import io.palyvos.provenance.util.AnankeSqliteQueryRunner;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.TimestampConverter;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink that writes {@link GenealogTuple} stream (including provenance) to PostgreSQL and records
 * latency.
 */
public class GenealogSqliteSink<T extends GenealogTuple> extends AbstractSqlSink<T> {

  private static Logger LOG = LoggerFactory.getLogger(GenealogSqliteSink.class);

  public static final String SQLITE_URL = "jdbc:sqlite:file::memory:?cache=shared";
  private static final String PRIMARY_KEY_CONSTRAINT = "PRIMARY KEY";
  private static final String EDGE_FOREIGN_KEY_CONSTRAINTS =
      ",FOREIGN KEY (sink) REFERENCES sink(id), "
          + "FOREIGN KEY (source) REFERENCES source(id)";
  private static final String CREATE_SOURCE = "CREATE TABLE IF NOT EXISTS source ("
      + "    id integer NOT NULL %s,"
      + "    ts integer NOT NULL,"
      + "    data blob NOT NULL,"
      + "    stimulus integer NOT NULL,"
      + "    stimulus_expired integer,"
      + "    sent integer DEFAULT false NOT NULL,"
      + "    expired integer DEFAULT 0 NOT NULL)";
  private static final String CREATE_SINK = "CREATE TABLE IF NOT EXISTS sink ("
      + "    id integer NOT NULL %s,"
      + "    ts integer NOT NULL,"
      + "    data blob NOT NULL,"
      + "    stimulus integer NOT NULL,"
      + "    sent integer DEFAULT false NOT NULL)";
  private static final String CREATE_EDGE = "CREATE TABLE IF NOT EXISTS edge ("
      + "    source bigint NOT NULL,"
      + "    sink bigint NOT NULL,"
      + "    stimulus bigint NOT NULL,"
      + "    sent boolean DEFAULT false NOT NULL %s)";
  private static final String CREATE_EXPIRED = "CREATE TABLE IF NOT EXISTS expired ("
      + "    id text NOT NULL,"
      + "    task integer NOT NULL,"
      + "    ts integer NOT NULL,"
      + "    PRIMARY KEY (id, task))";
  private static final String CREATE_INDEX_SOURCE_TS = "CREATE INDEX source_ts ON source (ts)";
  private static final String CREATE_INDEX_SOURCE_EXPIRED = "CREATE INDEX source_expired ON source (expired)";
  private static final String CREATE_INDEX_SOURCE_SENT = "CREATE INDEX source_sent ON source (sent)";
  private static final String CREATE_INDEX_SINK_SENT = "CREATE INDEX sink_sent ON sink (sent)";
  private static final String CREATE_INDEX_EDGE_SENT = "CREATE INDEX edge_sent ON edge (sent)";


  public static BasicDataSource DATA_SOURCE;
  private transient AnankeSqliteQueryRunner queryRunner;

  public GenealogSqliteSink(String name, long delayConstantMillis, ExperimentSettings settings,
      TimestampConverter timestampConverter) {
    super(settings, name, delayConstantMillis, timestampConverter);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    synchronized (GenealogSqliteSink.class) {
      // Global datasource to access same memory database
      if (DATA_SOURCE == null) {
        DATA_SOURCE = new BasicDataSource();
        DATA_SOURCE.setUrl(GenealogSqliteSink.SQLITE_URL);
        DATA_SOURCE.setDriverClassName("org.sqlite.JDBC");
        initDatabase();
      }
      try (Connection con = getConnection()) {
        con.setAutoCommit(false);
        initExpired(con);
        con.commit();
      }
    }
    queryRunner = new AnankeSqliteQueryRunner(settings, getRuntimeContext().getExecutionConfig());
    queryRunner.start();
  }

  private void initDatabase() throws SQLException {
    try (Connection con = DATA_SOURCE.getConnection()) {
      con.setAutoCommit(false);
      con.createStatement()
          .execute(withConstraint(CREATE_SOURCE, PRIMARY_KEY_CONSTRAINT, settings.uniqueSqlKeys()));
      con.createStatement()
          .execute(withConstraint(CREATE_SINK, PRIMARY_KEY_CONSTRAINT, settings.uniqueSqlKeys()));
      con.createStatement().execute(
          withConstraint(CREATE_EDGE, EDGE_FOREIGN_KEY_CONSTRAINTS, settings.uniqueSqlKeys()));
      con.createStatement().execute(CREATE_EXPIRED);
      con.createStatement().execute(CREATE_INDEX_SOURCE_TS);
      con.createStatement().execute(CREATE_INDEX_SOURCE_EXPIRED);
      con.createStatement().execute(CREATE_INDEX_SOURCE_SENT);
      con.createStatement().execute(CREATE_INDEX_SINK_SENT);
      con.createStatement().execute(CREATE_INDEX_EDGE_SENT);
      con.commit();
    }
  }

  private String withConstraint(String sql, String constraint, boolean enabled) {
    return String.format(sql, enabled ? constraint : "");
  }


  @Override
  public void close() throws Exception {
    queryRunner.stop();
    super.close();
  }

  @Override
  protected Connection getConnection() throws SQLException {
    return DATA_SOURCE.getConnection();
  }


}
