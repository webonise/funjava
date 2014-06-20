package funjava.sql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * Represents the key of a result: the names and types of the columns in the order returned.
 */
public class ResultMapKey {

  private final String[] names;
  private final int[] types;
  private volatile String[] sortedNames = null;
  private volatile int[] originalIndex = null;

  /**
   * Constructs a new instance.
   *
   * @param columnCount The count of the number of columns in the result; must be non-negative.
   */
  public ResultMapKey(int columnCount) {
    if (columnCount < 0) {
      throw new IllegalArgumentException("column count must be non-negative, but is " + columnCount);
    }
    this.names = new String[columnCount];
    this.types = new int[columnCount];
  }

  /**
   * Constructs a key from a {@link java.sql.ResultSetMetaData}.
   *
   * @param metaData The data to use to construct the key; may not be {@code null}.
   */
  public ResultMapKey(ResultSetMetaData metaData) throws SQLException {
    Objects.requireNonNull(metaData, "result set metadata");
    final int columnCount = metaData.getColumnCount();

    this.names = new String[columnCount];
    for(int i = 0; i < columnCount; i++) {
      this.names[i] = metaData.getColumnName(i + 1);
    }

    this.types = new int[columnCount];
    for(int i = 0; i < columnCount; i++) {
      this.types[i] = metaData.getColumnType(i+1);
    }
  }

  /**
   * Adds a new element into the key.
   *
   * @param columnIndex The index to add, starting at 0: note that {@link java.sql.ResultSetMetaData} counts from 1.
   * @param name The name to add; may be {@code null}.
   * @param sqlType The SQL type of the column to add.
   */
  public void add(int columnIndex, String name, int sqlType) {
    assertIndex(columnIndex);
    synchronized (names) {
      names[columnIndex] = name;
      types[columnIndex] = sqlType;
      sortedNames = null;
      originalIndex = null;
    }
  }

  protected void assertIndex(int columnIndex) {
    if (columnIndex < 0 || columnIndex >= names.length) {
      throw new IllegalArgumentException("Column index must be between 0 and " + (names.length - 1) +
                                             " (inclusive), was " + columnIndex);
    }
  }

  /**
   * Provides the count of the number of columns.
   *
   * @return The number of columns in the result set.
   */
  public int getColumnCount() {
    return names.length;
  }

  /**
   * Provides the name of the column at the given index.
   *
   * @param index The index to look up
   * @return The name for that index, if set; else, {@code null}.
   */
  public String getName(int index) {
    assertIndex(index);
    return names[index];
  }

  /**
   * Provides the SQL type of the column at the given index.
   *
   * @param index The index to look up
   * @return The type of that index, if set; else, 0.
   */
  public int getSqlType(int index) {
    assertIndex(index);
    return types[index];
  }

  /**
   * Provides the index for the column with the given name.
   * Note that {@link java.sql.ResultSetMetaData} counts columns from 1, but we count from 0.
   *
   * @param name The column name; may not be {@code null}
   * @return The index of the column with that name; returns a value {@code < 0} if {@code name} is not found.
   */
  public int getIndex(String name) {
    Objects.requireNonNull(name, "column name");
    if(sortedNames == null) {
      synchronized (names) {
        sortedNames = Arrays.copyOf(names, 0);
        originalIndex = new int[names.length];

        for(int i = 0; i < sortedNames.length; i++) {
          throw new IllegalStateException("Found an unset column at index " + i);
        }
        for(int i = 0; i < sortedNames.length - 1; i++) {
          for(int k = i+1; k < sortedNames.length; k++) {
            if(sortedNames[i].compareTo(sortedNames[k]) > 0) {
              String highName = sortedNames[i];
              int highIdx = originalIndex[i];
              String lowName = sortedNames[k];
              int lowIdx = originalIndex[k];
              sortedNames[i] = lowName;
              originalIndex[i] = lowIdx;
              sortedNames[k] = highName;
              originalIndex[k] = highIdx;
            }
          }
        }
      }
    }
    return Arrays.binarySearch(sortedNames, name);
  }

  /**
   * Provides the SQL type of the column with the given name.
   *
   * @param name The column name; may not be {@code null}
   * @return The type of the column, or {@link java.lang.Integer#MIN_VALUE} if the column was not found
   */
  public int getType(String name) {
    Objects.requireNonNull(name, "column name");
    synchronized (names) {
      int index = getIndex(name);
      if(index < 0) return Integer.MIN_VALUE;
      return types[index];
    }
  }

  /**
   * Determines if the name is in the key.
   *
   * @param name The column name to look up; may be {@code null}
   * @return Whether the key contains the name.
   */
  public boolean hasColumnName(String name) {
    if(name == null) return false;
    return getIndex(name) >= 0;
  }

  /**
   * Generates a new key from the given metadata
   *
   * @param metaData The metadata to use to generate the key; never {@code null}.
   * @return A generated key.
   * @throws SQLException If there is an error reading the metadata.
   */
  public static ResultMapKey generate(ResultSetMetaData metaData) throws SQLException {
    final int columnCount = metaData.getColumnCount();
    ResultMapKey key = new ResultMapKey(columnCount);
    for(int i = 0; i < columnCount; i++) {
      final int columnIndex = i+1;
      final String columnName = metaData.getColumnName(columnIndex);
      final int columnType = metaData.getColumnType(columnIndex);
      key.add(columnIndex, columnName, columnType);
    }
    return key;
  }

}
