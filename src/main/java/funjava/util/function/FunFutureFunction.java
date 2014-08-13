package funjava.util.function;

import java.util.*;
import java.util.concurrent.*;

/**
 * An extension to the {@link java.util.function.Function} interface with various useful methods.
 */
public interface FunFutureFunction<A, B> extends FunFunction<A, Future<B>> {

  /**
   * Creates a {@code FunFunction} from a {@code Function}. If the function is a {@code FunFunction} already, just
   * casts it; otherwise, wraps the function.
   *
   * @param f   The function to convert; cannot be {@code null}.
   * @param <A> The argument type.
   * @param <B> The return type.
   * @return The appropriately typed function; never {@code null}.
   */
  public static <A, B> FunFutureFunction<A, B> of(FunFunction<A, Future<B>> f) {
    Objects.requireNonNull(f, "function to convert");
    if (f instanceof FunFutureFunction) {
      return FunFutureFunction.class.cast(f);
    } else {
      return f::apply;
    }
  }


  /**
   * Given a function that produces a future, unwrap the future by waiting for the given timeout.
   *
   * @param timeoutMagnitude The magnitude of the timeout
   * @param timeoutUnits     The units of the timeout
   * @return A function that will wait for the results
   */
  public default FunFunction<A, B> joinAsync(long timeoutMagnitude, TimeUnit timeoutUnits
  ) {
    return a -> {
      try {
        return apply(a).get(timeoutMagnitude, timeoutUnits);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting for result", e);
      } catch (ExecutionException e) {
        throw new RuntimeException("Underlying task threw an exception", e);
      } catch (TimeoutException e) {
        throw new RuntimeException("Task did not resolve in " + timeoutMagnitude + " " + timeoutUnits, e);
      }
    };
  }

}
