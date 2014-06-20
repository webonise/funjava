package funjava.sql;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Functional interface for {@link java.sql.CallableStatement}. Instead of being an actual statement, this class
 * retains the information of how to create the statement, and executes the statement operation asynchronously within
 * the context of a callback.
 */
public class FunCallableStatement extends FunStatementBase<CallableStatement> {

  /**
   * Generates a provider that uses {@link java.sql.Connection#prepareCall(String)}.
   *
   * @param sql The SQL to prepare; never {@code null}
   * @return A provider that uses the given SQL to prepare a statement.
   */
  public static Function<Connection, CallableStatement> getProvider(String sql) {
    Objects.requireNonNull(sql, "prepared statement SQL");
    return c -> {
      try {
        return c.prepareCall(sql);
      } catch (SQLException e) {
        throw new RuntimeException("Error preparing SQL", e);
      }
    };
  }


  /**
   * Generates a provider that uses {@link java.sql.Connection#prepareCall(String, int, int)}.
   *
   * @param sql The SQL to prepare; never {@code null}
   * @param resultSetType The result set type to use; never {@code null}
   * @param resultSetConcurrency The result set concurrency to use; never {@code null}
   * @return A provider that will create a prepared statement for the given SQL returning a result set of the given type and concurrency.
   */
  public static Function<Connection,CallableStatement> getProvider(String sql, ResultSetType resultSetType, ResultSetConcurrency resultSetConcurrency) {
    Objects.requireNonNull(sql, "SQL to prepare");
    Objects.requireNonNull(resultSetType, "result set type");
    Objects.requireNonNull(resultSetConcurrency, "result set concurrency");
    return c -> {
      try {
        return c.prepareCall(sql, resultSetType.getValue(), resultSetConcurrency.getValue());
      } catch (SQLException e) {
        throw new RuntimeException("Error preparing statement", e);
      }
    };
  }

  /**
   * Generates a provider that uses {@link java.sql.Connection#prepareCall(String, int, int, int)}.
   *
   * @param sql The SQL to prepare; never {@code null}
   * @param resultSetType The result set type to use; never {@code null}
   * @param resultSetConcurrency The result set concurrency to use; never {@code null}
   * @param resultSetHoldability The result set holdability to use; never {@code null}
   * @return A provider that will prepare a statement using the given SQL with the result set type, concurrency, and holdability configured
   */
  public static Function<Connection,CallableStatement> getProvider(String sql, ResultSetType resultSetType, ResultSetConcurrency resultSetConcurrency, ResultSetHoldability resultSetHoldability) {
    Objects.requireNonNull(sql, "SQL to prepare");
    Objects.requireNonNull(resultSetType, "result set type");
    Objects.requireNonNull(resultSetConcurrency, "result set concurrency");
    Objects.requireNonNull(resultSetHoldability, "result set holdability");
    return c -> {
      try {
        return c.prepareCall(sql, resultSetType.getValue(), resultSetConcurrency.getValue(), resultSetHoldability.getValue());
      } catch (SQLException e) {
        throw new RuntimeException("Error preparing statement", e);
      }
    };
  }

  /**
   * Constructs an instance that will rely on the provider to generate statements, and does no further configuration.
   *
   * @param connection The functional connection; never {@code null}.
   * @param provider     The means of generating a {@link java.sql.CallableStatement}; never {@code null}.
   */
  public FunCallableStatement(Supplier<Connection> connection, Function<Connection, CallableStatement> provider) {
    super(connection, provider);
  }

  /**
   * Constructs an instance that will use the {@link #getProvider(String)}} provider.
   *
   * @param connection The functional connection; never {@code null}.
   * @param sql The SQL to prepare; never {@code null}
   */
  public FunCallableStatement(Supplier<Connection> connection, String sql) {
    super(connection, getProvider(sql));
  }


  /**
   * Constructs an instance that will use the provider to generate statements, and use the given means of configuring
   * the statement.
   *
   * @param connection   The functional connection; never {@code null}.
   * @param provider     The means of generating a {@link java.sql.PreparedStatement}; never {@code null}.
   * @param configurator The means of configuring a generated {@link java.sql.PreparedStatement}; never {@code null}.
   */
  public FunCallableStatement(Supplier<Connection> connection, Function<Connection, CallableStatement> provider, Consumer<CallableStatement> configurator) {
    super(connection, provider, configurator);
  }

}
