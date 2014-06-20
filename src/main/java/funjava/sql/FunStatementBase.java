package funjava.sql;

import funjava.util.concurrent.DrawingIterator;
import funjava.util.function.Functions;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * Functional interface for children of {@link java.sql.Statement}. Instead of being an actual statement, this class
 * retains the information of how to create the statement, and executes the statement operation asynchronously within
 * the context of a callback.
 */
public abstract class FunStatementBase<T extends Statement> implements Supplier<T> {

  private final FunConnection connection;
  private final Function<Connection, T> provider;
  private final Consumer<T> configurator;

  /**
   * Constructs an instance that will rely on the provider to generate statements, and does no further configuration.
   *
   * @param connection The functional connection; never {@code null}.
   * @param provider   The provider
   */
  public FunStatementBase(Supplier<Connection> connection, Function<Connection, T> provider) {
    this(connection, provider, Functions.doNothingConsumer());
  }

  /**
   * Constructs an instance that will use the provider to generate statements, and use the given means of configuring
   * the statement.
   *
   * @param connection   The functional connection; never {@code null}.
   * @param provider     The means of generating a {@link java.sql.Statement}; never {@code null}.
   * @param configurator The means of configuring a generated {@link java.sql.Statement}; never {@code null}.
   */
  public FunStatementBase(Supplier<Connection> connection, Function<Connection, T> provider, Consumer<T> configurator) {
    Objects.requireNonNull(connection, "functional connection");
    if(connection instanceof FunConnection) {
      this.connection = FunConnection.class.cast(connection);
    } else {
      this.connection = new FunConnection(connection);
    }
    Objects.requireNonNull(provider, "statement provider");
    this.provider = provider;
    Objects.requireNonNull(configurator, "statement configurator");
    this.configurator = configurator;
  }

  protected static boolean supportsBatch(Statement s) {
    try {
      DatabaseMetaData metaData = s.getConnection().getMetaData();
      return metaData.supportsBatchUpdates();
    } catch (SQLException e) {
      return false;
    }
  }

  /**
   * Uses {@link FunConnection#withConnection(java.util.function.Function)} to generate a connection,
   * then creates the statement, configures it, and executes the given callback within that connection context.
   *
   * @param callback The callback to execute; never {@code null}.
   * @param <U>      The type of value returned by the callback.
   * @return A future that will resolve to the return value of the callback.
   */
  public <U> Future<U> withStatement(Function<T, U> callback) {
    Objects.requireNonNull(callback, "statement callback");
    return connection.withConnection(c -> {
      try (T s = provider.apply(c)) {
        configurator.accept(s);
        return callback.apply(s);
      } catch (SQLException e) {
        throw new RuntimeException("Error executing statement", e);
      }
    });
  }

  /**
   * Uses {@link FunConnection#withConnection(java.util.function.Function)} to generate a connection,
   * then creates the statement, configures it, and executes the given callback within that connection context.
   * The {@link funjava.sql.FunConnection} that powers {@code this} is also passed into the callback. If you are looking
   * for access to the raw {@link java.sql.Connection}, then use {@link java.sql.Statement#getConnection()}.
   *
   * @param callback The callback to execute; never {@code null}.
   * @param <U>      The type of value returned by the callback.
   * @return A future that will resolve to the return value of the callback.
   */
  public <U> Future<U> withStatement(BiFunction<T, FunConnection, U> callback) {
    Objects.requireNonNull(callback, "statement callback");
    return connection.withConnection(c -> {
      try (T s = provider.apply(c)) {
        configurator.accept(s);
        return callback.apply(s, connection);
      } catch (SQLException e) {
        throw new RuntimeException("Error executing statement", e);
      }
    });
  }

  /**
   * Provides access to the functional connection that powers this statement.
   *
   * @return the functional connection; never {@code null}
   */
  public FunConnection getConnection() {
    return connection;
  }

  /**
   * Provides an unmanaged statement. The caller is responsible for closing the statement and its attached connection.
   *
   * @return A statement
   */
  @Override
  public T get() {
    T it = provider.apply(getConnection().get());
    configurator.accept(it);
    return it;
  }

  /**
   * Given a mapping from a statement to a {@link java.sql.ResultSet}, return a {@link java.util.stream.Stream} of
   * {@link funjava.sql.ResultMap} instances. The stream is populated asynchronously.
   *
   * @param fetch The function to generate the results to stream; never {@code null}
   * @return The stream of results; never {@code null}
   */
  public Stream<ResultMap> streamResults(Function<T,ResultSet> fetch) {
    final BlockingQueue<ResultMap> q = new LinkedBlockingQueue<>();
    final Future<Void> future = withStatement(s -> {
      final ResultSet rs = fetch.apply(s);
      FunConnection.readResultSetIntoQueue(rs, q);
      return null;
    });
    DrawingIterator<ResultMap> it = new DrawingIterator<>(future, q);
    Spliterator<ResultMap> spliterator = Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.IMMUTABLE);
    return StreamSupport.stream(spliterator, false);
  }


}

