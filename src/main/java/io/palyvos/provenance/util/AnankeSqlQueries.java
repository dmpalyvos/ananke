package io.palyvos.provenance.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

enum AnankeSqlQueries {
  UNIQUE_KEYS_POSTGRES {
    @Override
    ResultSet sinkQuery(Connection connection) throws SQLException {
      return connection.createStatement()
          .executeQuery("SELECT id, ts, stimulus, data FROM sink WHERE sent = false FOR UPDATE");
    }

    @Override
    void updateSink(Connection connection) throws SQLException {
      connection.createStatement()
          .execute("UPDATE sink SET sent = true WHERE sent = false");
    }

    @Override
    ResultSet sourceQuery(Connection connection) throws SQLException {
      return connection.createStatement()
          .executeQuery("SELECT id, ts, stimulus, data FROM source WHERE sent = false FOR UPDATE");
    }

    @Override
    void updateSource(Connection connection) throws SQLException {
      connection.createStatement()
          .execute("UPDATE source SET sent = true WHERE sent = false");
    }

    @Override
    ResultSet edgeQuery(Connection connection) throws SQLException {
      return connection.createStatement()
          .executeQuery("SELECT source, sink, stimulus FROM edge WHERE sent = false FOR UPDATE");
    }

    @Override
    void updateEdge(Connection connection) throws SQLException {
      connection.createStatement()
          .execute("UPDATE edge SET sent = true WHERE sent = false");
    }

    @Override
    ResultSet ackQuery(Connection connection) throws SQLException {
      return connection.createStatement()
          .executeQuery("SELECT id, ts, stimulus_expired FROM source "
              + "WHERE ts < (SELECT MIN(ts) FROM expired) AND expired = false FOR UPDATE");
    }

    @Override
    void updateAck(Connection connection) throws SQLException {
      connection.createStatement().execute("UPDATE source SET expired = true "
          + "WHERE ts < (SELECT MIN(ts) FROM expired) AND expired = false");
    }

  },
  DUPLICATE_KEYS_POSTGRES {
    @Override
    ResultSet sinkQuery(Connection connection) throws SQLException {
      return UNIQUE_KEYS_POSTGRES.sinkQuery(connection);
    }

    @Override
    void updateSink(Connection connection) throws SQLException {
      UNIQUE_KEYS_POSTGRES.updateSink(connection);
    }

    @Override
    ResultSet sourceQuery(Connection connection) throws SQLException {
      return connection.createStatement().executeQuery(
          "SELECT DISTINCT ON (source.id) source.id, source.ts, source.stimulus, source.data FROM source, usource "
              + "WHERE usource.id = source.id ORDER BY source.id, stimulus");
    }

    @Override
    void updateSource(Connection connection) throws SQLException {
      UNIQUE_KEYS_POSTGRES.updateSource(connection);
    }

    @Override
    ResultSet edgeQuery(Connection connection) throws SQLException {
      return UNIQUE_KEYS_POSTGRES.edgeQuery(connection);
    }

    @Override
    void updateEdge(Connection connection) throws SQLException {
      UNIQUE_KEYS_POSTGRES.updateEdge(connection);
    }

    @Override
    ResultSet ackQuery(Connection connection) throws SQLException {
      return connection.createStatement().executeQuery(
          "SELECT DISTINCT ON (source.id) source.id, source.ts, source.stimulus_expired, source.data FROM source, usource "
              + "WHERE usource.id = source.id "
              + "AND source.ts < (SELECT MIN(ts) FROM expired) "
              + "ORDER BY source.id, source.stimulus_expired");
    }

    @Override
    void updateAck(Connection connection) throws SQLException {
      UNIQUE_KEYS_POSTGRES.updateAck(connection);
    }

    @Override
    void prepareDatabase(Connection connection) throws SQLException {
      connection.createStatement().execute("CREATE TEMP TABLE usource ON COMMIT DROP AS "
          + "(SELECT id "
          + "FROM source GROUP BY id "
          + "HAVING bool_or(sent) = false AND bool_or(expired) = false)");
    }

  },
  UNIQUE_KEYS_SQLITE {
    @Override
    ResultSet sinkQuery(Connection connection) throws SQLException {
      return connection.createStatement().executeQuery("SELECT id, ts, stimulus, data FROM sink");
    }

    @Override
    void updateSink(Connection connection) throws SQLException {
      connection.createStatement().execute("DELETE FROM sink");
    }

    @Override
    ResultSet sourceQuery(Connection connection) throws SQLException {
      return connection.createStatement()
          .executeQuery("SELECT id, ts, stimulus, data FROM source WHERE sent = false");
    }

    @Override
    void updateSource(Connection connection) throws SQLException {
      UNIQUE_KEYS_POSTGRES.updateSource(connection);
    }

    @Override
    ResultSet edgeQuery(Connection connection) throws SQLException {
      return connection.createStatement().executeQuery("SELECT source, sink, stimulus FROM edge");
    }

    @Override
    void updateEdge(Connection connection) throws SQLException {
      connection.createStatement().execute("DELETE FROM edge");
    }

    @Override
    ResultSet ackQuery(Connection connection) throws SQLException {
      return connection.createStatement()
          .executeQuery("SELECT id, ts, stimulus_expired FROM source "
              + "WHERE ts < (SELECT MIN(ts) FROM expired) AND expired = false");
    }

    void updateAck(Connection connection) throws SQLException {
      connection.createStatement()
          .execute("DELETE FROM source WHERE ts < (SELECT MIN(ts) FROM expired)");
    }
  },
  DUPLICATE_KEYS_SQLITE {
    @Override
    ResultSet sinkQuery(Connection connection) throws SQLException {
      return UNIQUE_KEYS_SQLITE.sinkQuery(connection);
    }

    @Override
    void updateSink(Connection connection) throws SQLException {
      UNIQUE_KEYS_SQLITE.updateSink(connection);
    }

    @Override
    ResultSet sourceQuery(Connection connection) throws SQLException {
      return connection.createStatement().executeQuery(
          "SELECT source.id, MIN(source.ts) AS ts, MIN(source.stimulus) AS stimulus, FIRST_VALUE(source.data) "
              + "OVER (ORDER BY source.stimulus) AS data "
              + "FROM source "
              + "GROUP BY source.id HAVING MAX(source.sent) = false");
    }

    @Override
    void updateSource(Connection connection) throws SQLException {
      UNIQUE_KEYS_SQLITE.updateSource(connection);
    }

    @Override
    ResultSet edgeQuery(Connection connection) throws SQLException {
      return UNIQUE_KEYS_SQLITE.edgeQuery(connection);
    }

    @Override
    void updateEdge(Connection connection) throws SQLException {
      UNIQUE_KEYS_SQLITE.updateEdge(connection);
    }

    @Override
    ResultSet ackQuery(Connection connection) throws SQLException {
      return connection.createStatement().executeQuery(
          "SELECT source.id, MIN(source.ts), MIN(source.stimulus_expired) "
              + "FROM source WHERE ts < (SELECT MIN(ts) FROM expired) "
              + "GROUP BY source.id HAVING MAX(source.sent) = true");
    }

    @Override
    void updateAck(Connection connection) throws SQLException {
      UNIQUE_KEYS_SQLITE.updateAck(connection);
    }

  };


  abstract ResultSet sinkQuery(Connection connection) throws SQLException;

  abstract void updateSink(Connection connection) throws SQLException;

  abstract ResultSet sourceQuery(Connection connection) throws SQLException;

  abstract void updateSource(Connection connection) throws SQLException;

  abstract ResultSet edgeQuery(Connection connection) throws SQLException;

  abstract void updateEdge(Connection connection) throws SQLException;

  abstract ResultSet ackQuery(Connection connection) throws SQLException;

  abstract void updateAck(Connection connection) throws SQLException;

  void prepareDatabase(Connection connection) throws SQLException {
  }

}
