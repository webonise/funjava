package funjava.util.function;

import java.util.concurrent.*;

public interface FunFutureSupplier<A> extends FunSupplier<Future<A>> {

  static <A> FunFutureSupplier<A> of(FunSupplier<Future<A>> supplier) {
    if (supplier instanceof FunFutureSupplier) {
      return FunFutureSupplier.class.cast(supplier);
    } else {
      return supplier::get;
    }
  }

  /**
   * Given a supplier that supplies items asynchronously, create a synchronous supplier by joining it into the current
   * thread within the given timeout.
   *
   * @param timeoutMagnitude The magnitude of the timeout.
   * @param timeoutUnits     The units of the timeout.
   * @return A supplier that looks synchronous.
   */
  public default FunSupplier<A> joinAsync(long timeoutMagnitude, TimeUnit timeoutUnits) {
    return () -> {
      try {
        return get().get(timeoutMagnitude, timeoutUnits);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting for supplier", e);
      } catch (ExecutionException e) {
        throw new RuntimeException("Supplier threw an exception", e);
      } catch (TimeoutException e) {
        throw new RuntimeException("Supplier did not provide an element within the timeout", e);
      }
    };
  }

}
