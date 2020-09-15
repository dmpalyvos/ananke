package io.palyvos.provenance.util;


import io.palyvos.provenance.genealog.GenealogSqliteSink;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.ExecutionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnankeSqliteQueryRunner {

  public static final boolean WRITE_TO_FILE = true;
  private static Logger LOG = LoggerFactory.getLogger(AnankeSqliteQueryRunner.class);
  private final ExperimentSettings settings;
  private final ExecutionConfig executionConfig;
  private transient Thread thread;
  private transient Runner runner;
  private static boolean initialized = false;


  public AnankeSqliteQueryRunner(ExperimentSettings settings,
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
    synchronized (AnankeSqliteQueryRunner.class) {
      if (initialized) {
        return;
      }
      AnankeSqlQueries queries =
          settings.uniqueSqlKeys() ? AnankeSqlQueries.UNIQUE_KEYS_SQLITE
              : AnankeSqlQueries.DUPLICATE_KEYS_SQLITE;
      this.runner = new Runner("sqlite", settings, executionConfig, WRITE_TO_FILE, queries);
      this.thread = new Thread(runner);
      thread.setName("Sqlite-Query-Runner");
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


    public Runner(String name, ExperimentSettings settings,
        ExecutionConfig executionConfig, boolean writeToFile,
        AnankeSqlQueries queries) {
      super(name, settings, executionConfig, writeToFile, queries);
    }

    @Override
    protected Connection getConnection() throws SQLException {
      return GenealogSqliteSink.DATA_SOURCE
          .getConnection();
    }


  }
}
