package io.palyvos.provenance.util;

import java.sql.SQLException;

@FunctionalInterface
interface CheckedSQLConsumer<T> {
  void accept(T t) throws SQLException;
}
