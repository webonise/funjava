package funjava.sql

import funjava.FunJava
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicLong

/**
 * Base specification for databases.
 */
class DatabaseDrivenSpecBase extends Specification {

  static final AtomicLong ctr = new AtomicLong(0L)
  static final String CONNECT_STRING_BASE = "jdbc:h2:mem:test"
  static {
    Class.forName("org.h2.Driver");
  }

  String connectString = "${CONNECT_STRING_BASE}_${Long.toHexString(ctr.incrementAndGet())}"

  Connection connection1, connection2

  Connection createConnection() {
    DriverManager.getConnection(connectString)
  }

  /**
   * Alias for {@link #getFunConnection1()} for readability.
   */
  FunConnection getFunConnection() {
    return funConnection1
  }

  /**
   * Provides a {@link FunConnection} that uses {@link #connection1}.
   */
  FunConnection getFunConnection1() {
    return new FunConnection(connection1)
  }

  /**
   * Provides a {@link FunConnection} that uses {@link #connection2}.
   */
  FunConnection getFunConnection2() {
    return new FunConnection(connection2)
  }

  /**
   * Alias for {@link #connection1} for readability
   */
  Connection getConnection() {
    return connection1
  }

  void withConnection(Closure c) {
    Connection conn = null
    try {
      conn = createConnection()
      c.call(conn)
    } finally {
      conn?.close()
    }
  }

  void executeStatement(String sql) {
    withConnection { Connection conn ->
      Statement s = null
      try {
        s = conn.createStatement()
        s.execute(sql)
      } finally {
        s?.close()
      }
    }
  }

  Future<?> withConnectionAsync(Closure c) {
    return executeAsync {
      withConnection(c)
    }
  }

  Future<?> executeStatementAsync(String sql) {
    return executeAsync {
      executeStatement(sql)
    }
  }

  Future<?> executeAsync(Closure c) {
    return FunJava.executor.submit(c as Runnable)
  }

  def setup() {
    List<Future<?>> futures = []

    // We will explicitly close the database
    futures << executeStatementAsync("SET DB_CLOSE_DELAY -1")

    // Assign the two connections to work with
    futures << executeAsync { connection1 = createConnection() }
    futures << executeAsync { connection2 = createConnection() }

    // Make sure we have completed the DB_CLOSE_DELAY statement
    futures*.get()
  }

  /**
   * Creates a large table with 64 columns named {@code column_i} for {@code i = 0..63}, and {@code rows} number of
   * rows (defaults to 10000).
   *
   * @param rows The number of rows; anything less than zero is equivalent to zero
   * @return the name of the table
   */
  String largeTable(int rows = 10000) {
    String tableName = "big_table_${ctr.incrementAndGet()}"
    List<Future<?>> futures = []

    // Create a large table
    Future<?> createLargeTableFuture = executeStatementAsync("CREATE TABLE $tableName ()")
    futures << createLargeTableFuture
    List<Future<?>> columnFutures = []
    64.times { i ->
      columnFutures << executeAsync {
        createLargeTableFuture.get()
        executeStatement("ALTER TABLE $tableName ADD COLUMN column_$i VARCHAR(255)")
      }
    }
    futures.addAll(columnFutures)

    Math.max(0, rows).times { k ->
      int i = k % columnFutures.size()
      Future<?> columnFuture = columnFutures[i]
      futures << executeAsync {
        columnFuture.get()
        executeStatement("INSERT INTO $tableName (column_$i) VALUES ('${Long.toHexString(k)}')")
      }
    }

    futures*.get()
    return tableName
  }

  def "sanity check"() {
    expect:
    true
  }

  def "make small table"() {
    given:
    smallTable()

    expect:
    true
  }

  def "make large table"() {
    given:
    largeTable()

    expect:
    true
  }

/**
 * Creates a small table with three columns named {@code foo}, {@code bar}, and {@code baz}, and
 * {@code rows} number of rows (defaults to 10).
 *
 * @param rows The number of rows; anything less than zero is equivalent to zero
 * @return the name of the table
 */
  String smallTable(int rows = 10) {
    String tableName = "small_table_${ctr.incrementAndGet()}"

    List<Future<?>> futures = []

    // Create a small table
    Future<?> createSmallTableFuture = executeStatementAsync(
        "CREATE TABLE $tableName ( foo INTEGER, bar INTEGER, baz INTEGER)"
    )
    Math.max(0, rows).times { i ->
      futures << executeAsync {
        createSmallTableFuture.get()
        executeStatement("INSERT INTO $tableName (foo,bar,baz) VALUES ($i, ${i + 1}, ${i + 2})")
      }
    }
    futures*.get()
  }

  def cleanup() {
    // Close the connections
    List<Future<?>> closeFutures = [connection1, connection2].collect { Connection conn ->
      executeAsync { conn.close() }
    }

    // Shutdown the database to free the memory
    withConnection { Connection conn ->
      executeAsync { // We can't use anything that depends on instance state here
        closeFutures*.get()
        conn.createStatement().execute("shutdown")
      }
    }
  }


}
