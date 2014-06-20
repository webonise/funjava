package funjava.sql;

import java.sql.ResultSet;

/**
 * The result set concurrency options. See {@link java.sql.Connection#createStatement(int, int)}, and
 * {@link java.sql.ResultSet#CONCUR_READ_ONLY} and {@link java.sql.ResultSet#CONCUR_UPDATABLE}.
 */
public enum ResultSetConcurrency {
  READ_ONLY(ResultSet.CONCUR_READ_ONLY),
  UPDATABLE(ResultSet.CONCUR_UPDATABLE);

  final int value;

  private ResultSetConcurrency(int value) {
    this.value = value;
  }

  /**
   * The raw constant value.
   *
   * @return The value of the {@link java.sql.ResultSet} constant.
   */
  public int getValue() {
    return value;
  }
}
