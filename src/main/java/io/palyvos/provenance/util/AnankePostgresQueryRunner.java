package io.palyvos.provenance.util;


import io.palyvos.provenance.genealog.GenealogPostgresSink;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.ExecutionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnankePostgresQueryRunner {

  public static final boolean WRITE_TO_FILE = false;
  private static Logger LOG = LoggerFactory.getLogger(AnankePostgresQueryRunner.class);
  private final ExperimentSettings settings;
  private final ExecutionConfig executionConfig;
  private transient Thread thread;
  private transient Runner runner;
  private static boolean initialized = false;

  public AnankePostgresQueryRunner(ExperimentSettings settings,
      ExecutionConfig executionConfig) {
    Validate.notNull(settings, "settings");
    this.settings = settings;
    this.executionConfig = executionConfig;
  }

  /**
   * WARNING: Can be safely called multiple times, but only from one JVM. Otherwise there is a
   * possibility for concurrency issues!
   */
  public void start() {
    synchronized (AnankePostgresQueryRunner.class) {
      if (initialized) {
        return;
      }
      AnankeSqlQueries queries =
          settings.uniqueSqlKeys() ? AnankeSqlQueries.UNIQUE_KEYS_POSTGRES
              : AnankeSqlQueries.DUPLICATE_KEYS_POSTGRES;
      this.runner = new Runner("postgres", settings, executionConfig, WRITE_TO_FILE, queries);
      this.thread = new Thread(runner);
      thread.setName("Postgres-Query-Runner");
      thread.start();
      initialized = true;
    }
  }

  public void stop() {
    if (runner != null) {
      runner.stop();
    }
  }

  private static class Runner extends AbstractDatabaseQueryRunner {

    private transient BasicDataSource dataSource;

    public Runner(String name, ExperimentSettings settings,
        ExecutionConfig executionConfig, boolean writeToFile,
        AnankeSqlQueries queries) {
      super(name, settings, executionConfig, writeToFile, queries);
      dataSource = new BasicDataSource();
      dataSource.setUrl(GenealogPostgresSink.POSTGRES_URL);
      dataSource.setUsername(GenealogPostgresSink.POSTGRES_USER);
      dataSource.setPassword(GenealogPostgresSink.POSTGRES_PASSWORD);
      dataSource.setDriverClassName("org.postgresql.Driver");
      dataSource.setInitialSize(1);
    }

    @Override
    protected Connection getConnection() throws SQLException {
      return dataSource.getConnection();
    }

  }
}
