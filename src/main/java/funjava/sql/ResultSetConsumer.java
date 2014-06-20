package funjava.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * Given a {@link java.sql.ResultSet}, read the {@code ResultSet} into a queue of {@link funjava.sql.ResultMap}
 * objects.
 */
public class ResultSetConsumer implements Consumer<ResultSet> {

  private final BlockingQueue<ResultMap> queue;

  /**
   * Consumes values into the given queue.
   *
   * @param results The queue where results should be placed; never {@code null}.
   */
  public ResultSetConsumer(BlockingQueue<ResultMap> results) {
    Objects.requireNonNull(results, "the queue where we should place results");
    this.queue = results;
  }

  /**
   * Provides the queue that results will be written into.
   *
   * @return the queue where results are being placed; never {@code null}
   */
  public BlockingQueue<ResultMap> getQueue() {
    return queue;
  }

  /**
   * Reads the argument fully, writing into the queue.
   *
   * @param resultSet The result set to consume; never {@code null}.
   * @see #getQueue()
   */
  @Override
  public void accept(ResultSet resultSet) {
    Objects.requireNonNull(resultSet, "result set");
    try {
      final ResultMapKey key = ResultMapKey.generate(resultSet.getMetaData());
      while (resultSet.next()) {
        final ResultMap map = new ResultMap(key);
        map.load(resultSet);
        queue.add(map);
      }
    } catch(SQLException e) {
      throw new RuntimeException("Error reading result set into the queue", e);
    }
  }
}
