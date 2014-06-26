package funjava.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * A map of database query results. This map is an unmodifiable map keyed off both the column names and the result set
 * indexes (starting from 0). Values can be assigned into the result using {@link #add(int, Object)} and {@link
 * #add(String, Object)}, but they must be for values permitted by the {@link funjava.sql.ResultMapKey}.
 */
public class ResultMap extends AbstractMap<Object, Object> {

  private final ResultMapKey key;
  private final Object[] results;

  /**
   * Constructs a new map.
   *
   * @param key The key to the results; never {@code null}.
   */
  public ResultMap(ResultMapKey key) {
    Objects.requireNonNull(key, "result map key");
    this.key = key;
    results = new Object[key.getColumnCount()];
  }

  /**
   * Provides direct access to the key for these results.
   *
   * @return The key for these results; never {@code null}.
   */
  public ResultMapKey getMapKey() {
    return key;
  }

  /**
   * Adds a result for the given column name.
   *
   * @param columnName The column name to add; may not be {@code null}, and must be in the key.
   * @param value      The value to add, which may be {@code null}.
   */
  public void add(String columnName, Object value) {
    Objects.requireNonNull(columnName, "column name");
    int index = key.getIndex(columnName);
    if (index < 0) {
      throw new IllegalArgumentException("No column named '" + columnName + "' found");
    }
    results[index] = value;
  }

  @Override
  public boolean containsValue(Object value) {
    for (final Object result : results) {
      if (result == value) return true;
      if (result != null && result.equals(value)) return true;
    }
    return false;
  }

  @Override
  public boolean containsKey(Object column) {
    if (column == null) {
      return false;
    } else if (column instanceof CharSequence) {
      return key.hasColumnName(column.toString());
    } else if (column instanceof Number) {
      int index = Number.class.cast(column).intValue();
      return index >= 0 && index < results.length;
    } else {
      return false;
    }
  }

  @Override
  public Object get(Object column) {
    // Find the index
    final int index;
    if (column == null) {
      return null;
    } else if (column instanceof CharSequence) {
      index = key.getIndex(column.toString());
    } else if (column instanceof Number) {
      index = Number.class.cast(column).intValue();
    } else {
      return null;
    }

    // Return the result at that index (if it's within the bounds)
    if (index >= 0 && index < results.length) {
      return results[index];
    } else {
      return null;
    }
  }

  @Override
  public Set<Entry<Object, Object>> entrySet() {
    Set<Entry<Object, Object>> toReturn = new HashSet<>();
    for (int i = 0; i < results.length; i++) {
      Object result = results[i];
      SimpleImmutableEntry<Object, Object> nameEntry = new SimpleImmutableEntry<>(key.getName(i), result);
      SimpleImmutableEntry<Object, Object> indexEntry = new SimpleImmutableEntry<>(i, result);
      toReturn.add(nameEntry);
      toReturn.add(indexEntry);
    }
    return toReturn;
  }

  /**
   * Loads is map from the given result set.
   *
   * @param resultSet The result set to load; never {@code null}.
   * @throws java.sql.SQLException If an exception occurs reading the result set.
   */
  public void load(final ResultSet resultSet) throws SQLException {
    Objects.requireNonNull(resultSet, "result set to load");
    for (int i = 0; i < results.length; i++) {
      this.add(i, resultSet.getObject(i + 1));
    }
  }

  /**
   * Adds a result at the given column index. Note that {@link java.sql.ResultSet} and {@link
   * java.sql.ResultSetMetaData} count from 1, but this class counts from 0.
   *
   * @param columnIndex The index to add, which must be no less than 0 and less than {@code key.getColumnCount()}.
   * @param value       The value to add, which may be {@code null}.
   */
  public void add(int columnIndex, Object value) {
    key.assertIndex(columnIndex);
    results[columnIndex] = value;
  }

}
