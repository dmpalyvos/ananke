package io.palyvos.provenance.genealog;

import io.palyvos.provenance.util.AnankePostgresQueryRunner;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.TimestampConverter;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.Validate;
import org.apache.flink.configuration.Configuration;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink that writes {@link GenealogTuple} stream (including provenance) to PostgreSQL and records
 * latency.
 */
public class GenealogPostgresSink<T extends GenealogTuple> extends AbstractSqlSink<T> {

  public static final String POSTGRES_DROP_ALL_SQL = "postgres-drop-all.sql";
  public static final String POSTGRES_DUPLICATES_SQL = "postgres-duplicates.sql";
  public static final String POSTGRES_UNIQUE_KEYS_SQL = "postgres-unique.sql";
  private static Logger LOG = LoggerFactory.getLogger(GenealogPostgresSink.class);

  public static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/ananke";
  public static final String POSTGRES_USER = "postgres";
  public static final String POSTGRES_PASSWORD = "ananke";
  private static boolean initialized = false;
  private transient BasicDataSource dataSource;
  private transient AnankePostgresQueryRunner queryRunner;

  public GenealogPostgresSink(String name, long delayConstantMillis, ExperimentSettings settings,
      TimestampConverter timestampConverter) {
    super(settings, name, delayConstantMillis, timestampConverter);
    Validate.notBlank(name, "name");
    Validate.notNull(settings, "settings");
    Validate.notNull(timestampConverter, "timestampConverter");
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    dataSource = new BasicDataSource();
    dataSource.setUrl(POSTGRES_URL);
    dataSource.setUsername(POSTGRES_USER);
    dataSource.setPassword(POSTGRES_PASSWORD);
    dataSource.setDriverClassName("org.postgresql.Driver");
    dataSource.setInitialSize(1);
    synchronized (GenealogPostgresSink.class) {
      // WARNING
      // Will not work for distributed sinks
      // To handle this case, there should be a retry logic for transactions
      // and slightly different table creation
      // but locking is dirty and easy for experimental purposes
      if (!initialized) {
        initDatabase();
        initialized = true;
      }
      try (Connection con = getConnection()) {
        con.setAutoCommit(false);
        initExpired(con);
        con.commit();
      }
    }
    queryRunner = new AnankePostgresQueryRunner(settings, getRuntimeContext().getExecutionConfig());
    queryRunner.start();
  }

  private void initDatabase() throws SQLException {
    try (Connection con = getConnection()) {
      con.setAutoCommit(false);
      ScriptRunner sr = new ScriptRunner(con);
      runSqlFromResources(sr, POSTGRES_DROP_ALL_SQL);
      runSqlFromResources(sr,
          settings.uniqueSqlKeys() ? POSTGRES_UNIQUE_KEYS_SQL : POSTGRES_DUPLICATES_SQL);
      con.commit();
    }
  }

  private void runSqlFromResources(ScriptRunner sr, String sql) {
    sr.runScript(new InputStreamReader(
        getClass().getClassLoader().getResourceAsStream(sql)));
  }

  @Override
  public void close() throws Exception {
    super.close();
    queryRunner.stop();
  }

  @Override
  protected Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

}
