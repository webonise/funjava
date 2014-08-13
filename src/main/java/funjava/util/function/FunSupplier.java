package funjava.util.function;

import funjava.FunJava;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * A {@link java.util.function.Supplier} with more functional bells and whistles.
 */
public interface FunSupplier<T> extends Supplier<T> {

  public static <T> FunSupplier<T> of(Supplier<T> supplier) {
    Objects.requireNonNull(supplier, "supplier to test");
    if (supplier instanceof FunSupplier) {
      return FunSupplier.class.cast(supplier);
    } else {
      return supplier::get;
    }
  }

  /**
   * Given a future, generate a supplier that resolves that future by waiting for it on the given timeout.
   *
   * @param future           The future to wait for; never {@code null}
   * @param timeoutMagnitude The magnitude of the timeout
   * @param timeoutScale     The unit of the timeout
   * @param <T>              The return type of the future
   * @return A supplier that provides the future's value each time {@link java.util.function.Supplier#get()} is called.
   */
  public static <T> FunSupplier<T> futureSupplier(Future<T> future, long timeoutMagnitude, TimeUnit timeoutScale) {
    Objects.requireNonNull(future, "future to resolve");
    return () -> {
      try {
        return future.get(timeoutMagnitude, timeoutScale);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting on future", e);
      } catch (ExecutionException e) {
        throw new RuntimeException("Future execution threw an exception", e);
      } catch (TimeoutException e) {
        throw new RuntimeException("Timeout while waiting for future to resolve", e);
      }
    };
  }


  /**
   * Given a supplier, returns a supplier that supplies the result asynchronously.
   *
   * @return A supplier that calls the nested supplier asynchronously.
   */
  public default FunFutureSupplier<T> supplyAsync() {
    return () -> FunJava.getExecutor().submit((Callable) this::get);
  }


}
