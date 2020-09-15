package io.palyvos.provenance.util;

import java.sql.SQLException;

public interface SQLCloseable extends AutoCloseable {

  @Override
  void close() throws SQLException;
}