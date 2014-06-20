package funjava.sql;

import java.sql.ResultSet;

/**
 * The values of the result set cursor holdability. See {@link java.sql.Connection#createStatement(int, int, int)} and
 * {@link java.sql.ResultSet#HOLD_CURSORS_OVER_COMMIT} and {@link java.sql.ResultSet#CLOSE_CURSORS_AT_COMMIT}
 */
public enum ResultSetHoldability {
  HOLD_CURSORS(ResultSet.HOLD_CURSORS_OVER_COMMIT),
  CLOSE_CURSORS(ResultSet.CLOSE_CURSORS_AT_COMMIT);

  private final int value;

  ResultSetHoldability(int value) {
    this.value = value;
  }

  /**
   * Provide the raw constant value corresponding to this enum.
   *
   * @return The raw constant value.
   */
  public int getValue() {
    return value;
  }
}
