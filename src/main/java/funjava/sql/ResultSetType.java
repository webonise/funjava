package funjava.sql;

import java.sql.ResultSet;

/**
 * Enumeration representing the result set type. See {@link java.sql.Connection#createStatement(int, int)},
 * and {@link ResultSet.TYPE_FORWARD_ONLY}, {@link java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE}, and
 * {@link java.sql.ResultSet.TYPE_SCROLL_SENSITIVE}.
 */
public enum ResultSetType {
  TYPE_FORWARD_ONLY(ResultSet.TYPE_FORWARD_ONLY),
  TYPE_SCROLL_INSENSITIVE(ResultSet.TYPE_SCROLL_INSENSITIVE),
  TYPE_SCROLL_SENSITIVE(ResultSet.TYPE_SCROLL_SENSITIVE);

  final int value;

  private ResultSetType(int value) {
    this.value = value;
  }

  /**
   * Value of the {@link ResultSet} constant corresponding to this enum.
   *
   * @return The constant value.
   */
  public int getValue() {
    return value;
  }


}
