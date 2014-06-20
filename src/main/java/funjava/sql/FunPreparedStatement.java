package funjava.sql;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * Functional interface for {@link java.sql.PreparedStatement}. Instead of being an actual statement, this class retains
 * the information of how to create the statement, and executes the statement operation asynchronously within the
 * context of a callback.
 */
public class FunPreparedStatement extends FunStatementBase<PreparedStatement> {

  /**
   * Constructs an instance that will use the provider to generate statements, and use the given means of configuring
   * the statement.
   *
   * @param connection   The functional connection; never {@code null}.
   * @param provider     The means of generating a {@link java.sql.PreparedStatement}; never {@code null}.
   * @param configurator The means of configuring a generated {@link java.sql.PreparedStatement}; never {@code null}.
   */
  public FunPreparedStatement(Supplier<Connection> connection, Function<Connection, PreparedStatement> provider, Consumer<PreparedStatement> configurator) {
    super(connection, provider, configurator);
  }

  /**
   * Constructs an instance using the given connection and the {@link #getProvider(String)} provider.
   *
   * @param connection The functional connection; never {@code null}.
   * @param sql        The SQL to use to prepare the statement; never {@code null}
   */
  public FunPreparedStatement(Supplier<Connection> connection, String sql) {
    this(connection, getProvider(sql));
  }

  /**
   * Constructs an instance that will rely on the provider to generate statements, and does no further configuration.
   *
   * @param connection The functional connection; never {@code null}.
   * @param provider   The provider
   */
  public FunPreparedStatement(Supplier<Connection> connection, Function<Connection, PreparedStatement> provider) {
    super(connection, provider);
  }

  /**
   * Generates a provider that uses {@link java.sql.Connection#prepareStatement(String)}.
   *
   * @param sql The SQL to prepare; never {@code null}
   * @return A provider that uses the given SQL to prepare a statement.
   */
  public static Function<Connection, PreparedStatement> getProvider(String sql) {
    Objects.requireNonNull(sql, "prepared statement SQL");
    return c -> {
      try {
        return c.prepareStatement(sql);
      } catch (SQLException e) {
        throw new RuntimeException("Error preparing SQL", e);
      }
    };
  }

  /**
   * Generates a provider that uses {@link java.sql.Connection#prepareStatement(String, int)}.
   *
   * @param sql          The SQL to use to prepare the statement; never {@code null}
   * @param generateKeys If {@code true}, use {@link java.sql.Statement#RETURN_GENERATED_KEYS}; if {@code false}, use
   *                     {@link java.sql.Statement#NO_GENERATED_KEYS}.
   * @return A provider that uses the SQL and whose key generation is properly configured
   */
  public static Function<Connection, PreparedStatement> getProvider(String sql, boolean generateKeys) {
    Objects.requireNonNull(sql, "SQL to prepare");
    return c -> {
      try {
        return c.prepareStatement(sql, generateKeys ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
      } catch (SQLException e) {
        throw new RuntimeException("Error preparing SQL. Generated keys=" + generateKeys, e);
      }
    };
  }

  /**
   * Generates a provider that uses {@link java.sql.Connection#prepareStatement(String, int[])}.
   *
   * @param sql           The SQL to prepare; never {@code null}
   * @param columnIndexes an array of column indexes indicating the columns that should be returned from the inserted
   *                      row or rows; never {@code null}
   * @return a provider using the given SQL to create a statement that will return the columns at the given indexes
   */
  public static Function<Connection, PreparedStatement> getProvider(String sql, int[] columnIndexes) {
    Objects.requireNonNull(sql, "SQL to prepare");
    Objects.requireNonNull(columnIndexes, "indexes of columns to return");
    return c -> {
      try {
        return c.prepareStatement(sql, columnIndexes);
      } catch (SQLException e) {
        throw new RuntimeException("Error preparing SQL", e);
      }
    };
  }

  /**
   * Generates a provider that uses {@link java.sql.Connection#prepareStatement(String, String[])}.
   *
   * @param sql         The SQL to prepare; never {@code null}
   * @param columnNames an array of column names indicating the columns that should be returned from the inserted row or
   *                    rows; never {@code null}
   * @return a provider using the given SQL to create a statement that will return the columns with the given names
   */
  public static Function<Connection, PreparedStatement> getProvider(String sql, String[] columnNames) {
    Objects.requireNonNull(sql, "SQL to prepare");
    Objects.requireNonNull(columnNames, "names of columns to return");
    return c -> {
      try {
        return c.prepareStatement(sql, columnNames);
      } catch (SQLException e) {
        throw new RuntimeException("Error preparing SQL", e);
      }
    };
  }

  /**
   * Generates a provider that uses {@link java.sql.Connection#prepareStatement(String, int, int)}.
   *
   * @param sql                  The SQL to prepare; never {@code null}
   * @param resultSetType        The result set type to use; never {@code null}
   * @param resultSetConcurrency The result set concurrency to use; never {@code null}
   * @return A provider that will create a prepared statement for the given SQL returning a result set of the given type
   * and concurrency.
   */
  public static Function<Connection, PreparedStatement> getProvider(String sql, ResultSetType resultSetType, ResultSetConcurrency resultSetConcurrency) {
    Objects.requireNonNull(sql, "SQL to prepare");
    Objects.requireNonNull(resultSetType, "result set type");
    Objects.requireNonNull(resultSetConcurrency, "result set concurrency");
    return c -> {
      try {
        return c.prepareStatement(sql, resultSetType.getValue(), resultSetConcurrency.getValue());
      } catch (SQLException e) {
        throw new RuntimeException("Error preparing statement", e);
      }
    };
  }

  /**
   * Generates a provider that uses {@link java.sql.Connection#prepareStatement(String, int, int, int)}.
   *
   * @param sql                  The SQL to prepare; never {@code null}
   * @param resultSetType        The result set type to use; never {@code null}
   * @param resultSetConcurrency The result set concurrency to use; never {@code null}
   * @param resultSetHoldability The result set holdability to use; never {@code null}
   * @return A provider that will prepare a statement using the given SQL with the result set type, concurrency, and
   * holdability configured
   */
  public static Function<Connection, PreparedStatement> getProvider(String sql, ResultSetType resultSetType, ResultSetConcurrency resultSetConcurrency, ResultSetHoldability resultSetHoldability) {
    Objects.requireNonNull(sql, "SQL to prepare");
    Objects.requireNonNull(resultSetType, "result set type");
    Objects.requireNonNull(resultSetConcurrency, "result set concurrency");
    Objects.requireNonNull(resultSetHoldability, "result set holdability");
    return c -> {
      try {
        return c.prepareStatement(sql, resultSetType.getValue(), resultSetConcurrency.getValue(), resultSetHoldability.getValue());
      } catch (SQLException e) {
        throw new RuntimeException("Error preparing statement", e);
      }
    };
  }

  /**
   * Given a collection of arguments, and a function that can set the parameters of a {@link java.sql.PreparedStatement}
   * given an argument, execute a batch update. If the database does not support batching, falls back to using distinct
   * {@link java.sql.PreparedStatement#execute()} calls.
   *
   * @param batchArgs The arguments to use; never {@code null}.
   * @param setter    The function to set each argument; never {@code null}.
   * @param <A>       The type of the argument.
   * @return A handle to retrieve the results of the updates
   */
  public <A> Future<int[]> batchUpdate(Collection<A> batchArgs, BiConsumer<PreparedStatement, A> setter) {
    Objects.requireNonNull(batchArgs, "the arguments for assigning to the batch");
    Objects.requireNonNull(setter, "the function defining how to set the arguments");
    Iterator<A> it = batchArgs.iterator();
    ObjIntConsumer<PreparedStatement> argGenerator = (ps, idx) -> {
      setter.accept(ps, it.next());
    };
    return batchUpdate(batchArgs.size(), argGenerator);
  }

  /**
   * Given a SQL statement and a function for setting the parameters given the index, execute a batch update. If the
   * database does not support batching, falls back to using distinct {@link java.sql.PreparedStatement#execute()}
   * calls.  For each value {@code i, 0 <= i < batchSize}, the {@code setter} function will be called with the same
   * {@link java.sql.PreparedStatement} as the first argument, and the value of {@code i} for the second argument. It is
   * expected to assign the parameters of the statement.
   *
   * @param batchSize The size of the batch (must be non-negative).
   * @param setter    The function responsible for setting the prepared statements.
   * @return A handle to retrieve the results of the updates.
   */
  public Future<int[]> batchUpdate(int batchSize, ObjIntConsumer<PreparedStatement> setter) {
    if (batchSize < 0) throw new IllegalArgumentException("batch size must be non-negative, was " + batchSize);
    Objects.requireNonNull(setter, "function used to set prepared statement parameters");
    return withStatement(s ->
                             supportsBatch(s) ? realBatchUpdate(s, batchSize, setter) : fakeBatchUpdate(s, batchSize, setter)
    );
  }

  private int[] realBatchUpdate(PreparedStatement s, int batchSize, ObjIntConsumer<PreparedStatement> setter) {
    for (int i = 0; i < batchSize; i++) {
      setter.accept(s, batchSize);
      try {
        s.addBatch();
      } catch (SQLException e) {
        throw new RuntimeException("error when attempting to add batch parameters", e);
      }
    }
    try {
      return s.executeBatch();
    } catch (SQLException e) {
      throw new RuntimeException("error when executing batch", e);
    }
  }

  private int[] fakeBatchUpdate(PreparedStatement s, int batchSize, ObjIntConsumer<PreparedStatement> setter) {
    int[] toReturn = new int[batchSize];
    for (int i = 0; i < batchSize; i++) {
      setter.accept(s, i);
      try {
        toReturn[i] = s.executeUpdate();
      } catch (SQLException e) {
        throw new RuntimeException("Error when executing pseudo-batch update statement", e);
      }
    }
    return toReturn;
  }

  /**
   * Given a stream of arguments, and a function that can set the parameters of a {@link java.sql.PreparedStatement}
   * given an argument, execute a batch update. If the database does not support batching, falls back to using distinct
   * {@link java.sql.PreparedStatement#execute()} calls.
   *
   * @param batchArgs The arguments to use; never {@code null}.
   * @param setter    The function to set each argument; never {@code null}.
   * @param <A>       The type of the argument.
   * @return A handle to retrieve the results of the updates
   */
  public <A> Future<int[]> batchUpdate(Stream<A> batchArgs, BiConsumer<PreparedStatement, A> setter) {
    Objects.requireNonNull(batchArgs, "the arguments for assigning to the batch");
    Objects.requireNonNull(setter, "the function defining how to set the arguments");
    return withStatement(s -> supportsBatch(s) ? realBatchUpdate(s, batchArgs, setter) : fakeBatchUpdate(s, batchArgs, setter));
  }

  private <A> int[] realBatchUpdate(PreparedStatement s, Stream<A> batchArgs, BiConsumer<PreparedStatement, A> setter) {
    batchArgs.forEachOrdered(it -> {
      setter.accept(s, it);
      try {
        s.addBatch();
      } catch (SQLException e) {
        throw new RuntimeException("Error while adding arguments to the batch", e);
      }
    });
    try {
      return s.executeBatch();
    } catch (SQLException e) {
      throw new RuntimeException("Error while executing batch", e);
    }
  }

  private <A> int[] fakeBatchUpdate(PreparedStatement s, Stream<A> batchArgs, BiConsumer<PreparedStatement, A> setter) {
    ArrayList<Integer> results = new ArrayList<>();
    batchArgs.forEachOrdered(it -> {
      setter.accept(s, it);
      try {
        results.add(s.executeUpdate());
      } catch (SQLException e) {
        throw new RuntimeException("Error executing update on pseudo-batch", e);
      }
    });
    final int[] toReturn = new int[results.size()];
    Arrays.parallelSetAll(toReturn, results::get);
    return toReturn;
  }

  /**
   * Executes a {@link java.sql.PreparedStatement} using the given SQL, assigning the given parameters, and then relying
   * on the callback to iterate over and process the entire result set.
   *
   * @param args               The args to use; never {@code null}, but may be empty.
   * @param resultSetExtractor The callback that will process the result set
   * @param <A>                The value returned by the result set processing callback
   * @return A handle that will provide the result of the processing callback.
   */
  public <A> Future<A> queryToExtract(Object[] args, Function<ResultSet, A> resultSetExtractor) {
    Objects.requireNonNull(args, "SQL prepared statement arguments");
    Objects.requireNonNull(resultSetExtractor, "ResultSet extractor");
    return withStatement(s -> {
      assignArgs(s, args);
      final ResultSet rs;
      try {
        rs = s.executeQuery();
      } catch (SQLException e) {
        throw new RuntimeException("Error executing query", e);
      }
      try {
        return resultSetExtractor.apply(rs);
      } finally {
        try {
          rs.close();
        } catch (SQLException e) {
          throw new RuntimeException("Error closing result set", e);
        }
      }
    });
  }

  protected static void assignArgs(PreparedStatement s, Object[] args) {
    for (int i = 0; i < args.length; i++) {
      try {
        s.setObject(i + 1, args[i]);
      } catch (SQLException e) {
        throw new RuntimeException("Error setting index " + i + " to value: " + args[i], e);
      }
    }
  }

  /**
   * Executes a {@link java.sql.PreparedStatement} using the given SQL, assigning the given parameters, and then
   * returning the {@link ResultMap} containing the results.
   *
   * @param args The args to use; never {@code null}, but may be empty.
   * @return A stream of the results as {@link ResultMap} instances.
   */
  public Stream<ResultMap> queryToStream(final Object[] args) {
    return streamResults(s -> {
      assignArgs(s, args);
      try {
        return s.executeQuery();
      } catch (SQLException e) {
        throw new RuntimeException("Error while executing query");
      }
    });
  }

  public Stream<ResultMap> queryToStream(final Consumer<PreparedStatement> argSetter) {
    return streamResults(s -> {
      argSetter.accept(s);
      try {
        return s.executeQuery();
      } catch (SQLException e) {
        throw new RuntimeException("Error while executing query");
      }
    });
  }

  public <A> Future<A> queryToObject(final Object[] args, final Class<A> toReturn) {
    Objects.requireNonNull(toReturn, "class to cast the result to");
    return this.queryToObject(args, toReturn::cast);
  }

  public <A> Future<A> queryToObject(final Object[] args, final Function<Object, A> converter) {
    Objects.requireNonNull(args, "the arguments to assign");
    Objects.requireNonNull(converter, "the converter function");
    return withStatement(s -> {
      assignArgs(s, args);
      final ResultSet rs;
      try {
        rs = s.executeQuery();
      } catch (SQLException e) {
        throw new RuntimeException("Exception when executing query", e);
      }
      Object result;
      try {
        if (rs.next()) {
          result = rs.getObject(1);
        } else {
          result = null;
        }
        rs.close();
      } catch (SQLException e) {
        throw new RuntimeException("Exception when reading first result", e);
      }
      return result == null ? null : converter.apply(result);
    });
  }

  /**
   * Given a SQL and arguments, provide the first result.
   *
   * @param args The args to use; never {@code null}, but may be empty.
   * @return A handle that will provide the first result of the result set, or {@code null} if there were no results.
   */
  public Future<ResultMap> queryToResult(Object[] args) {
    Objects.requireNonNull(args, "the arguments to assign");
    return withStatement(s -> {
      assignArgs(s, args);
      final ResultSet rs;
      try {
        rs = s.executeQuery();
      } catch (SQLException e) {
        throw new RuntimeException("Error executing query", e);
      }
      try {
        if (rs.next()) {
          final ResultMapKey key = ResultMapKey.generate(rs.getMetaData());
          final ResultMap result = new ResultMap(key);
          result.load(rs);
          return result;
        } else {
          return null;
        }
      } catch (SQLException e) {
        throw new RuntimeException("Error extracting results", e);
      }
    });
  }
}
