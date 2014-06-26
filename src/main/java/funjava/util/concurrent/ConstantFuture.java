package funjava.util.concurrent;

import java.util.concurrent.*;

/**
 * An implementation of {@link Future} that simply returns a constant.
 */
public class ConstantFuture<V> implements Future<V> {

  private final V value;

  /**
   * Constructor.
   *
   * @param value The value to return; may be {@code null}.
   */
  public ConstantFuture(V value) {
    this.value = value;
  }


  /**
   * {@inheritDoc}
   *
   * @param mayInterruptIfRunning Ignored.
   * @return {@code false}, always
   */
  @Override
  public boolean cancel(final boolean mayInterruptIfRunning) {
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code false}, always
   */
  @Override
  public boolean isCancelled() {
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true}, always
   */
  @Override
  public boolean isDone() {
    return true;
  }

  /**
   * Immediately returns the constant value.
   *
   * @return the constant value
   */
  @Override
  public V get() {
    return value;
  }

  /**
   * Immediately returns the constant value.
   *
   * @param timeout Ignored.
   * @param unit    Ignored.
   * @return the constant value
   */
  @Override
  public V get(final long timeout, final TimeUnit unit) {
    return value;
  }
}
