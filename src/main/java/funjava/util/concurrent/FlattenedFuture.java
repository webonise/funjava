package funjava.util.concurrent;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * Provides a view of a future-of-a-future as a single-layer future.
 */
public class FlattenedFuture<V> implements Future<V> {

  private final Future<Future<V>> it;

  /**
   * Unwraps the given future.
   *
   * @param wrappedFuture The future to unwrap; never {@code null}
   */
  public FlattenedFuture(Future<Future<V>> wrappedFuture) {
    Objects.requireNonNull(wrappedFuture, "the future to unwrap");
    this.it = wrappedFuture;
  }

  /**
   * Cancels both the outer and inner future.
   * <p>
   * {@inheritDoc}
   *
   * @param mayInterruptIfRunning Passed to the futures
   * @return whether both futures were able to be cancelled.
   */
  @Override
  public boolean cancel(final boolean mayInterruptIfRunning) {
    return it.cancel(mayInterruptIfRunning) && applyInside(f -> f.cancel(mayInterruptIfRunning));
  }

  private <A> A applyInside(Function<Future<?>, A> f) {
    Future<?> target;
    try {
      target = it.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      target = it;
    }
    return f.apply(target);
  }

  /**
   * @return {@code true} if the outer future is cancelled, or the outer future is done and the inner future is
   * cancelled; otherwise, {@code false}
   */
  @Override
  public boolean isCancelled() {
    return it.isCancelled() || (isDone() && applyInside(Future::isCancelled));
  }

  /**
   * @return {@code true} if both the inner and outer future are done.
   */
  @Override
  public boolean isDone() {
    return it.isDone() && applyInside(Future::isDone);
  }

  /**
   * Waits on both futures to resolve.
   *
   * @return The result of the inner future.
   */
  @Override
  public V get() throws InterruptedException, ExecutionException {
    return it.get().get();
  }

  /**
   * Waits no longer than the given timeout for both futures to resolve. Note that the timeout covers both futures, so
   * the maximum wait time is {@code 1 x timeout}, not {@code 2 x timeout}
   *
   * @param timeout The timeout to wait; must be {@code > 0}
   * @param unit    The units to use; never {@code null}
   * @return The result of the inner future
   */
  @Override
  public V get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException,
                                                                   TimeoutException {
    Objects.requireNonNull(unit, "timeout units");
    if (timeout <= 0) {
      throw new IllegalArgumentException("timeout magnitude must be >0");
    }
    long endTime = unit.toMillis(timeout) + System.currentTimeMillis();
    Future<V> inside = it.get(timeout, unit);
    long millisLeft = Math.max(1, endTime - System.currentTimeMillis());
    return inside.get(millisLeft, TimeUnit.MILLISECONDS);
  }
}
