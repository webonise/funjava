package funjava.sql;

import funjava.FunJava;
import funjava.util.function.FunConsumer;
import funjava.util.function.FunSupplier;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.logging.*;
import java.util.stream.*;

/**
 * Functional interface for {@link java.sql.Connection}. Instead of being an actual database connection, this class
 * retains the information on how to get a database connection, and executes the database operation asynchronously
 * within the context of a callback.
 * <p>
 * There are a few advantages to using this approach over the more standard JDBC approach. The first is that this is
 * much more concurrency-friendly: a FunConnection is always safe to be used no matter what is going on in other
 * threads; also, a FunConnection delegates the slow database communication to a background thread, which means the
 * application's processing threads are not blocked while it attempts to talk to the database.
 */
public class FunConnection implements Supplier<Connection> {

  private final FunSupplier<Connection> provider;
  private final FunConsumer<Connection> configurator;

  /**
   * Constructs an instance that will rely on the provider to generate connections, and does no configuration.
   *
   * @param provider The means of generating a {@link java.sql.Connection}; never {@code null}.
   */
  public FunConnection(Supplier<Connection> provider) {
    this(provider, FunConsumer.doNothingConsumer());
  }

  /**
   * Constructs an instance that will use the provider to generate connections, and use the given means of configuring
   * that connection.
   *
   * @param provider     The means of generating a {@link Connection}; never {@code null}.
   * @param configurator The means of configuring a generated {@link Connection}; never {@code null}.
   */
  public FunConnection(Supplier<Connection> provider, Consumer<Connection> configurator) {
    Objects.requireNonNull(provider, "Connection provider");
    this.provider = FunSupplier.of(provider);
    Objects.requireNonNull(configurator, "Connection configurator");
    this.configurator = FunConsumer.of(configurator);
  }

  public static final FunConsumer<Connection> closeQuietly() {
    return FunConnection::closeQuietly;
  }

  /**
   * Generates a provider that acquires a connection by looking up the given name in the {@link
   * javax.naming.InitialContext}. The name is presumed to be the entire name, which probably looks like {@code
   * java:/com/env/jdbc/MyDataSourceName}.
   *
   * @param name The name to look up; never {@code null}
   * @return A provider that looks up the given name in the initial context
   */
  public static Supplier<Connection> getJndiProvider(final String name) {
    return () -> {
      final Context ctx;
      try {
        ctx = new InitialContext();
      } catch (NamingException e) {
        throw new RuntimeException("Could not construct InitialContext", e);
      }
      return getJndiProvider(ctx, name).get();
    };
  }

  /**
   * Generates a provider that acquires a connection by looking up the given name in the given context. The name is
   * presumed to be the entire name, which probably looks like {@code java:/comp/env/jdbc/MyDataSourceName}.
   *
   * @param jndiCtx The context to use; never {@code null}
   * @param name    The name to look up; never {@code null}
   * @return A provider that looks up the given name in the given context.
   */
  public static Supplier<Connection> getJndiProvider(final Context jndiCtx, final String name) {
    Objects.requireNonNull(jndiCtx, "JNDI Context");
    Objects.requireNonNull(name, "JNDI Name");
    return () -> {
      final Object found;
      try {
        found = jndiCtx.lookup(name);
      } catch (NamingException e) {
        throw new RuntimeException("Could not look up " + name, e);
      }
      Objects.requireNonNull(found, "Found null at " + name + " in context " + jndiCtx);
      return Connection.class.cast(found);
    };
  }

  /**
   * Provides a connection via {@link javax.sql.DataSource#getConnection()}.
   *
   * @param ds The data source to use; never {@code null}
   * @return A supplier that uses the data source to fetch connections
   */
  public static Supplier<Connection> getDataSourceProvider(DataSource ds) {
    Objects.requireNonNull(ds, "data source");
    return () -> {
      try {
        return ds.getConnection();
      } catch (SQLException e) {
        throw new RuntimeException("Error while getting connection from data source", e);
      }
    };
  }

  /**
   * Provides a connection via {@link javax.sql.DataSource#getConnection(String, String)}.
   *
   * @param ds       The data source to use; never {@code null}
   * @param username The username to use; never {@code null}
   * @param password The password to use; never {@code null}
   * @return A supplier that uses the data source to fetch connections
   */
  public static Supplier<Connection> getDataSourceProvider(DataSource ds, String username, String password) {
    Objects.requireNonNull(ds, "data source");
    Objects.requireNonNull(username, "data source connection username");
    Objects.requireNonNull(password, "data source connection password");
    return () -> {
      try {
        return ds.getConnection(username, password);
      } catch (SQLException e) {
        throw new RuntimeException("Error while getting connection from data source for user=" + username);
      }
    };
  }

  /**
   * Provides a connection via {@link java.sql.DriverManager#getConnection(String)}.
   *
   * @param connString The connection string to use; never {@code null}
   * @return A supplier that uses that connection string.
   */
  public static Supplier<Connection> getJdbcProvider(String connString) {
    Objects.requireNonNull(connString, "connection string");
    return () -> {
      try {
        return DriverManager.getConnection(connString);
      } catch (SQLException e) {
        throw new RuntimeException("Error constructing connection using " + connString, e);
      }
    };
  }

  /**
   * Provides a connection via {@link java.sql.DriverManager#getConnection(String, java.util.Properties)}.
   *
   * @param connString The connection string to use; never {@code null}
   * @param info       The connection info to use; never {@code null}
   * @return A supplier that uses the connection string and connection info
   */
  public static Supplier<Connection> getJdbcProvider(String connString, Properties info) {
    Objects.requireNonNull(connString, "connection string");
    Objects.requireNonNull(info, "connection info");
    return () -> {
      try {
        return DriverManager.getConnection(connString, info);
      } catch (SQLException e) {
        throw new RuntimeException("Error constructing connection using " + connString + " and with info: " + info, e);
      }
    };
  }

  /**
   * Provides a connection via {@link java.sql.DriverManager#getConnection(String, String, String)}.
   *
   * @param connString The connection string to use; never {@code null}
   * @param user       The connection user to use; never {@code null}
   * @param password   The connection password to use; never {@code null}
   * @return A supplier that uses the connection string, user, and password.
   */
  public static Supplier<Connection> getJdbcProvider(String connString, String user, String password) {
    Objects.requireNonNull(connString, "connection string");
    Objects.requireNonNull(user, "connection user");
    Objects.requireNonNull(password, "connection password");
    return () -> {
      try {
        return DriverManager.getConnection(connString, user, password);
      } catch (SQLException e) {
        throw new RuntimeException("Error constructing connection using " + connString + " for user " + user + " " +
                                       "(password hidden)", e
        );
      }
    };
  }

  /**
   * Reads the given result set and writes its rows into the given queue as {@link funjava.sql.ResultMap} instances.
   *
   * @param rs The result set to read; never {@code null}
   * @param q  The queue to load; never {@code null}
   */
  protected static final void readResultSetIntoQueue(ResultSet rs, Queue<ResultMap> q) {
    Objects.requireNonNull(rs, "result set to read");
    Objects.requireNonNull(q, "queue to load");
    try {
      final ResultMapKey key = ResultMapKey.generate(rs.getMetaData());
      while (rs.next()) {
        final ResultMap map = new ResultMap(key);
        map.load(rs);
        q.add(map);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Error reading result set into queue", e);
    }
  }

  /**
   * Inspired by the idea of "currying", this takes a {@link java.util.function.BiFunction} whose first argument is a
   * connection, and generates a {@link java.util.function.Function} that will execute within the connection context,
   * and will return a future that will resolve to the result of the bifunction.
   *
   * @param function The callback to make with the connection; never {@code null}
   * @param <A>      The type of the other argument to the function to be called.
   * @param <B>      The type of the return value of the function to be called.
   * @return The function that create the connection context and asynchronously execute the callback.
   */
  public <A, B> Function<A, Future<B>> curry(BiFunction<Connection, A, B> function) {
    return it -> withConnection(c -> function.apply(c, it));
  }

  /**
   * In another thread, create a connection and pass it into the callback. The connection is opened immediately before
   * the callback is called and closed when the callback returns.
   *
   * @param callback The callback to execute with a connection; never {@code null}.
   * @param <T>      The type of value returned by the callback.
   * @return A {@link Future} that will resolve to the result of the callback.
   */
  public <T> Future<T> withConnection(final Function<Connection, T> callback) {
    Objects.requireNonNull(callback, "connection callback");
    return FunJava.getExecutor().submit(() -> {
          try (Connection c = get()) {
            return callback.apply(c);
          }
        }
    );
  }

  /**
   * Generates a new unmanaged connection. The caller is responsible for closing the connection that is returned.
   *
   * @return A database connection; never {@code null}.
   */
  @Override
  public Connection get() {
    Connection c = null;
    try {
      c = provider.get();
      Objects.requireNonNull(c, "database connection");
      configurator.accept(c);
      return c;
    } catch (RuntimeException re) {
      closeQuietly(c);
      throw re;
    } catch (Exception e) {
      closeQuietly(c);
      throw new RuntimeException("Error creating database connection", e);
    }
  }

  /**
   * Closes the connection quietly, logging any errors at the {@link java.util.logging.Level#FINE} level to the {@code
   * java.sql.Connection} logger. Note that this method can be coerced into a consumer by calling: {@code
   * FunConnection::closeQuiety}.
   *
   * @param c The connection to close; if {@code null}, this method does nothing.
   */
  public static final void closeQuietly(Connection c) {
    try {
      if (c != null) c.close();
    } catch (Exception ignored) {
      FunJava.getExecutor().submit(() -> {
            Logger logger = Logger.getLogger(Connection.class.getName());
            logger.log(Level.INFO, "Error while quietly closing connection", ignored);
          }
      );
    }
  }

  /**
   * Executes the given SQL, which is typically a DDL statement, and ignores the result. This uses the default settings
   * for a {@link java.sql.Statement}. If you would like more control, use {@link funjava.sql.FunStatement} directly.
   *
   * @param sql The SQL to execute; never {@code null}.
   * @return A handle for determining when the update is complete, and whether an exception was thrown.
   * @see funjava.sql.FunStatement#execute(String)
   */
  public Future<Void> execute(String sql) {
    Objects.requireNonNull(sql, "SQL to execute");
    return new FunStatement(this).execute(sql);
  }

  /**
   * Given a stream of SQL updates, executes them asynchronously using a single JDBC statement using batching. If the
   * database does not support batching, falls back to using distinct {@link java.sql.Statement#execute(String)} calls.
   * This uses the default configuration for a {@link java.sql.Statement}: for more configuration options, use {@link
   * funjava.sql.FunStatement} directly.
   *
   * @param sqlStream The stream of SQL to execute; never {@code null}.
   * @return A handle to retrieve the results of the updates.
   * @see funjava.sql.FunStatement#batchUpdate(java.util.stream.Stream)
   */
  public Future<int[]> batchUpdate(Stream<String> sqlStream) {
    Objects.requireNonNull(sqlStream, "SQL stream to execute");
    return new FunStatement(this).batchUpdate(sqlStream);
  }

  /**
   * Given a SQL statement and a function for setting the parameters given the index, execute a batch update. If the
   * database does not support batching, falls back to using distinct {@link java.sql.PreparedStatement#execute()}
   * calls. This uses the default configuration for a {@link java.sql.PreparedStatement}; for more configuration
   * options, use {@link funjava.sql.FunPreparedStatement} directly.
   *
   * @param sql       The SQL to execute; never {@code null}.
   * @param batchSize The size of the batch (must be non-negative).
   * @param setter    The function responsible for setting the prepared statements.
   * @return A handle to retrieve the results of the updates.
   * @see funjava.sql.FunPreparedStatement#batchUpdate(int, java.util.function.ObjIntConsumer)
   */
  public Future<int[]> batchUpdate(String sql, int batchSize, ObjIntConsumer<PreparedStatement> setter) {
    Objects.requireNonNull(sql, "SQL template to execute");
    if (batchSize < 0) throw new IllegalArgumentException("batch size must be non-negative, was " + batchSize);
    Objects.requireNonNull(setter, "function used to set prepared statement parameters");
    return new FunPreparedStatement(this, sql).batchUpdate(batchSize, setter);
  }

  /**
   * Given a SQL statement, a collection of arguments, and a function that can set the parameters of a {@link
   * java.sql.PreparedStatement} given an argument, execute a batch update. If the database does not support batching,
   * falls back to using distinct {@link java.sql.PreparedStatement#execute()} calls. This uses the default
   * configuration for a {@link java.sql.PreparedStatement}; for more configuration options, use {@link
   * funjava.sql.FunPreparedStatement} directly.
   *
   * @param sql       The SQL to execute; never {@code null}.
   * @param batchArgs The arguments to use; never {@code null}.
   * @param setter    The function to set each argument; never {@code null}.
   * @param <A>       The type of the argument.
   * @return A handle to retrieve the results of the updates
   * @see funjava.sql.FunPreparedStatement#batchUpdate(Collection, java.util.function.BiConsumer)
   */
  public <A> Future<int[]> batchUpdate(String sql, Collection<A> batchArgs, BiConsumer<PreparedStatement, A> setter) {
    Objects.requireNonNull(sql, "SQL template for batch");
    Objects.requireNonNull(batchArgs, "the arguments for assigning to the batch");
    Objects.requireNonNull(setter, "the function defining how to set the arguments");
    return new FunPreparedStatement(this, sql).batchUpdate(batchArgs, setter);
  }

  /**
   * Given a SQL statement, a stream of arguments, and a function that can set the parameters of a {@link
   * java.sql.PreparedStatement} given an argument, execute a batch update. If the database does not support batching,
   * falls back to using distinct {@link java.sql.PreparedStatement#execute()} calls. This uses the default
   * configuration for a {@link java.sql.PreparedStatement}; for more configuration options, use {@link
   * funjava.sql.FunPreparedStatement} directly.
   *
   * @param sql       The SQL to execute; never {@code null}.
   * @param batchArgs The arguments to use; never {@code null}.
   * @param setter    The function to set each argument; never {@code null}.
   * @param <A>       The type of the argument.
   * @return A handle to retrieve the results of the updates
   * @see funjava.sql.FunPreparedStatement#batchUpdate(Stream, java.util.function.BiConsumer)
   */
  public <A> Future<int[]> batchUpdate(String sql, Stream<A> batchArgs, BiConsumer<PreparedStatement, A> setter) {
    Objects.requireNonNull(sql, "SQL template for batch");
    Objects.requireNonNull(batchArgs, "the arguments for assigning to the batch");
    Objects.requireNonNull(setter, "the function defining how to set the arguments");
    return new FunPreparedStatement(this, sql).batchUpdate(batchArgs, setter);
  }

  /**
   * Executes a {@link java.sql.PreparedStatement} using the given SQL, assigning the given parameters, and then
   * returning the {@link ResultMap} containing the results.
   *
   * @param sql  The SQL to execute; never {@code null}.
   * @param args The args to use; never {@code null}, but may be empty.
   * @return A stream of the results as {@link funjava.sql.ResultMap} instances.
   */
  public Stream<ResultMap> queryToStream(String sql, Object[] args) {
    return new FunPreparedStatement(this, sql).queryToStream(args);
  }

  /**
   * Executes a {@link java.sql.PreparedStatement} using the given SQL, and calling the given function to assign the
   * parameters, and then returning the {@link ResultMap} containing the results.
   *
   * @param sql       The SQL to execute; never {@code null}.
   * @param argSetter The function that will assign the args to use; never {@code null}.
   * @return A stream of the results as {@link funjava.sql.ResultMap} instances.
   * @see funjava.util.function.FunConsumer#doNothingConsumer()
   */
  public Stream<ResultMap> queryToStream(String sql, Consumer<PreparedStatement> argSetter) {
    return new FunPreparedStatement(this, sql).queryToStream(argSetter);
  }

  /**
   * Executes a {@link java.sql.PreparedStatement} using the given SQL, assigning the given parameters, and then
   * relying
   * on the callback to iterate over and process the entire result set.
   *
   * @param sql                The SQL to execute; never {@code null}.
   * @param args               The args to use; never {@code null}, but may be empty.
   * @param resultSetExtractor The callback that will process the result set
   * @param <A>                The value returned by the result set processing callback
   * @return A handle that will provide the result of the processing callback.
   * @see funjava.sql.FunPreparedStatement#queryToExtract(Object[], Function)
   */
  public <A> Future<A> queryToExtract(String sql, Object[] args, Function<ResultSet, A> resultSetExtractor) {
    return new FunPreparedStatement(this, sql).queryToExtract(args, resultSetExtractor);
  }

  /**
   * Executes a {@link java.sql.PreparedStatement} using the given SQL, assigning the given parameters, and then
   * reading
   * the first column of the first returned result and casting it to the given type.
   *
   * @param sql      The SQL to execute; never {@code null}.
   * @param args     The args to use; never {@code null}, but may be empty.
   * @param toReturn The {@link Class} object representing the type to be returned; never {@code null}.
   * @param <A>      The type to be returned
   * @return A handle that will provide the result of casting the first column of the first returned value, or {@code
   * null} if there were no results.
   * @see Class#cast(Object)
   */
  public <A> Future<A> queryToObject(String sql, Object[] args, Class<A> toReturn) {
    return new FunPreparedStatement(this, sql).queryToObject(args, toReturn);
  }

  /**
   * Executes a {@link java.sql.PreparedStatement} using the given SQL, assigning the given parameters, and then
   * reading
   * the first column of the first returned result and using the converter to finesse it into the correct type.
   *
   * @param sql       The SQL to execute; never {@code null}.
   * @param args      The args to use; never {@code null}, but may be empty.
   * @param converter The function generating the type to be returned; never {@code null}.
   * @param <A>       The type to be returned
   * @return A handle that will provide the result of converting the first column of the first returned value, or {@code
   * null} if there were no results.
   */
  public <A> Future<A> queryToObject(String sql, Object[] args, Function<Object, A> converter) {
    return new FunPreparedStatement(this, sql).queryToObject(args, converter);
  }

  /**
   * Given a SQL and arguments, provide the first result.
   *
   * @param sql  The SQL to execute; never {@code null}.
   * @param args The args to use; never {@code null}, but may be empty.
   * @return A handle that will provide the first result of the result set, or {@code null} if there were no results.
   */
  public Future<ResultMap> queryToResult(String sql, Object[] args) {
    return new FunPreparedStatement(this, sql).queryToResult(args);
  }

}
