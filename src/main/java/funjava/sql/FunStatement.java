package funjava.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * Functional interface for {@link java.sql.Statement}. Instead of being an actual statement, this class retains the
 * information of how to create the statement, and executes the statement operation asynchronously within the context
 * of a callback.
 */
public class FunStatement extends FunStatementBase<Statement> {

  /**
   * Constructs an instance using the given {@link Connection} and {@link #getProvider()}. The caller is responsible
   * for closing the provided connection.
   *
   * @param connection The connection to use; never {@code null}
   */
  public FunStatement(Connection connection) {
    super(connection, getProvider());
  }

  /**
   * Generates a provider based on {@link java.sql.Connection#createStatement()}.
   *
   * @return A new provider which simply calls {@code createStatement()}
   */
  public static Function<Connection, Statement> getProvider() {
    return c -> {
      try {
        return c.createStatement();
      } catch (SQLException e) {
        throw new RuntimeException("Could not create statement", e);
      }
    };
  }

  /**
   * Constructs an instance using the given connection and {@link #getProvider()}.
   *
   * @param connection The connection supplier to use; never {@code null}.
   */
  public FunStatement(Supplier<Connection> connection) {
    super(connection, getProvider());
  }

  /**
   * Constructs an instance that will rely on the provider to generate statements, and does no further configuration.
   *
   * @param connection The functional connection; never {@code null}.
   * @param provider   The means of generating a {@link Statement}; never {@code null}.
   */
  public FunStatement(Supplier<Connection> connection, Function<Connection, Statement> provider) {
    super(connection, provider);
  }

  /**
   * Constructs an instance that will use the provider to generate statements, and use the given means of configuring
   * the statement.
   *
   * @param connection   The functional connection; never {@code null}.
   * @param provider     The means of generating a {@link Statement}; never {@code null}.
   * @param configurator The means of configuring a generated {@link Statement}; never {@code null}.
   */
  public FunStatement(Supplier<Connection> connection, Function<Connection, Statement> provider,
                      Consumer<Statement> configurator) {
    super(connection, provider, configurator);
  }

  /**
   * Generates a provider based on {@link java.sql.Connection#createStatement(int, int)}.
   *
   * @param resultSetType        the result set type
   * @param resultSetConcurrency the result set concurrency
   * @return A function providing a statement with the given result set type and concurrency
   */
  public static Function<Connection, Statement> getProvider(final ResultSetType resultSetType,
                                                            final ResultSetConcurrency resultSetConcurrency) {
    Objects.requireNonNull(resultSetType, "result set type");
    Objects.requireNonNull(resultSetConcurrency, "result set concurrency");
    return c -> {
      try {
        return c.createStatement(resultSetType.getValue(), resultSetConcurrency.getValue());
      } catch (SQLException e) {
        throw new RuntimeException("Could not create statement", e);
      }
    };
  }

  public static Function<Connection, Statement> getProvider(final ResultSetType resultSetType,
                                                            final ResultSetConcurrency resultSetConcurrency,
                                                            final ResultSetHoldability resultSetHoldability) {
    Objects.requireNonNull(resultSetType, "result set type");
    Objects.requireNonNull(resultSetConcurrency, "result set concurrency");
    Objects.requireNonNull(resultSetHoldability, "result set holdability");
    return c -> {
      try {
        return c.createStatement(resultSetType.getValue(), resultSetConcurrency.getValue(),
                                    resultSetHoldability.getValue());
      } catch (SQLException e) {
        throw new RuntimeException("Could not create statement", e);
      }
    };
  }

  /**
   * Executes the given SQL, which is typically a DDL statement, and ignores the result.
   *
   * @param sql The SQL to execute; never {@code null}.
   * @return A handle for determining when the update is complete, and whether an exception was thrown.
   */
  public Future<Void> execute(String sql) {
    Objects.requireNonNull(sql, "SQL to execute");
    return withStatement(s -> {
          try {
            s.execute(sql);
            return null;
          } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL", e);
          }
        }
    );
  }

  /**
   * Given a stream of SQL updates, executes them asynchronously in a single JDBC statement using batching.
   * If the database does not support batching, falls back to using distinct {@link java.sql.Statement#execute(String)}
   * calls.
   *
   * @param sqlStream The stream of SQL to execute; never {@code null}.
   * @return A handle to retrieve the results of the updates.
   */
  public Future<int[]> batchUpdate(Stream<String> sqlStream) {
    Objects.requireNonNull(sqlStream, "SQL stream to execute");
    return withStatement(s ->
                             supportsBatch(s) ? realBatchUpdate(s, sqlStream) : fakeBatchUpdate(s, sqlStream)
    );
  }

  private static int[] realBatchUpdate(Statement s, Stream<String> sqlStream) {
    sqlStream.forEach(sql -> {
          Objects.requireNonNull(sql, "individual SQL update statement to be added to batch");
          try {
            synchronized (s) {
              s.addBatch(sql);
            }
          } catch (SQLException e) {
            throw new RuntimeException("Error attempting to add SQL for batch update", e);
          }
        }
    );
    try {
      return s.executeBatch();
    } catch (SQLException e) {
      throw new RuntimeException("Error executing batch update", e);
    }

  }

  private static int[] fakeBatchUpdate(Statement s, Stream<String> sqlStream) {
    return sqlStream.mapToInt(sql -> {
          try {
            synchronized (s) {
              return s.executeUpdate(sql);
            }
          } catch (SQLException e) {
            throw new RuntimeException("Exception executing SQL statement in batch", e);
          }
        }
    ).toArray();
  }

}
