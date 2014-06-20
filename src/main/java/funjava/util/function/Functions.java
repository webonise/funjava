package funjava.util.function;

import funjava.FunJava;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * Various nifty kinds of {@link Consumer}, {@link java.util.function.Supplier}, etc.
 */
public class Functions {

  /**
   * Provides a consumer that does nothing.
   *
   * @param <T> The type to return.
   * @return A {@link java.util.function.Consumer} that does nothing.
   */
  public static <T> Consumer<T> doNothingConsumer() {
    return new Consumer<T>() {
      @Override
      public void accept(T t) {
        return;
      }
    };
  }

  /**
   * Given a future, generate a supplier that resolves that future by waiting for it on the given timeout.
   *
   * @param future           The future to wait for; never {@code null}
   * @param timeoutMagnitude The magnitude of the timeout
   * @param timeoutScale     The unit of the timeout
   * @param <T>              The return type of the future
   * @return A supplier that provides the future's value.
   */
  public static <T> Supplier<T> futureSupplier(Future<T> future, long timeoutMagnitude, TimeUnit timeoutScale) {
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
   * Taking a function of two arguments, split it into a function of one argument returning another one-argument
   * function.
   * <p>
   * In short, it makes {@code f:(a,b)->c} into {@code f:(a)->(b->c)}.
   *
   * @param f The function to curry; never {@code null}.
   * @return The curried function.
   */
  public <A, B, C> Function<A, Function<B, C>> curry(final BiFunction<A, B, C> f) {
    Objects.requireNonNull(f, "function to curry");
    return (A a) -> (B b) -> f.apply(a, b);
  }

  /**
   * Given a one-argument function that returns another function, convert it into a function taking two arguments.
   * <p>
   * In short, make {@code f:(a)->(b->c)} into {@code f:(a,b)->c}.
   *
   * @param f The function to uncurry; never {@code null}.
   * @return The uncurried function.
   */
  public <A, B, C> BiFunction<A, B, C> uncurry(Function<A, Function<B, C>> f) {
    Objects.requireNonNull(f, "function to curry");
    return (A a, B b) -> {
      Function<B, C> g = f.apply(a);
      Objects.requireNonNull(g, "function generated by applying argument");
      return g.apply(b);
    };
  }

  /**
   * Given a {@link java.util.function.Supplier}, convert it to a function that takes any argument and returns the
   * supplied value.
   *
   * @param s The supplier function; never {@code null}.
   * @return A function that ignores the argument and calls the supplier.
   */
  public <A, B> Function<A, B> functionify(Supplier<B> s) {
    Objects.requireNonNull(s, "supplier to wrap");
    return a -> s.get();
  }

  /**
   * Given a {@link java.util.function.Consumer}, convert it to a function that consumes the argument and returns {@code
   * null}.
   *
   * @param c The consumer function; never {@code null}.
   * @return A function that consumes the argument and returns {@code null}
   */
  public <A, B> Function<A, B> functionify(Consumer<A> c) {
    return functionify(c, null);
  }

  /**
   * Given a {@link java.util.function.Consumer} and {@link java.util.function.Supplier}, construct a function that
   * consumes the argument and supplies the result.
   *
   * @param c The consumer function; never {@code null}.
   * @param s The supplier function; never {@code null}.
   * @return A function that passes the argument to the consumer and calls the supplier to generate the return value.
   */
  public <A, B> Function<A, B> functionify(Consumer<A> c, Supplier<B> s) {
    Objects.requireNonNull(c, "consumer function");
    Objects.requireNonNull(s, "supplier function");
    return a -> {
      c.accept(a);
      return s.get();
    };
  }

  /**
   * Given a {@link java.util.function.Consumer}, convert it to a function that consumes the argument and returns the
   * provided value.
   *
   * @param c           The consumer function; never {@code null}.
   * @param returnValue The value to return.
   * @return A function that consumes the argument and returns the return value.
   */
  public <A, B> Function<A, B> functionify(Consumer<A> c, B returnValue) {
    Objects.requireNonNull(c, "consumer to wrap");
    return a -> {
      c.accept(a);
      return returnValue;
    };
  }

  /**
   * Given a function that takes an argument and returns a value, generate a consumer that takes the argument, calls the
   * function, and ignores the return value.
   *
   * @param f The function to convert; never {@code null}
   * @return A consumer that calls the function and ignores its return value.
   */
  public <A, B> Consumer<A> consumerify(Function<A, B> f) {
    Objects.requireNonNull(f, "function to wrap");
    return a -> f.apply(a);
  }

  /**
   * Given a function that takes an argument and returns a result, generate a supplier that, each time it is called,
   * will pass in the argument and return the result.
   *
   * @param f The function to convert; never {@code null}
   * @param a The argument to pass to the function
   * @return A supplier that calls the function with the argument each time
   */
  public <A, B> Supplier<B> supplierify(Function<A, B> f, A a) {
    return () -> f.apply(a);
  }

  /**
   * Given a function that takes an argument and returns a result, generate a supplier that, each time it is called,
   * will pass in {@code null} and return the result.
   *
   * @param f The function to convert; never {@code null}
   * @return A supplier that calls the function with the argument each time
   */
  public <A, B> Supplier<B> supplierify(Function<A, B> f) {
    Objects.requireNonNull(f, "function to convert");
    return () -> f.apply(null);
  }

  /**
   * Given a function, return a function that calls it asynchronously.
   *
   * @param f The function to wrap; never {@code null}
   * @return A function that calls the given function asynchronously
   * @see funjava.FunJava#getExecutor()
   */
  public <A, B> Function<A, Future<B>> executeAsync(Function<A, B> f) {
    Objects.requireNonNull(f, "function to wrap");
    return a -> FunJava.getExecutor().submit(() -> f.apply(a));
  }

  /**
   * Given a function that produces a future, unwrap the future by waiting for the given timeout.
   *
   * @param f                Function to wrap; never {@code null}
   * @param timeoutMagnitude The magnitude of the timeout
   * @param timeoutUnits     The units of the timeout
   * @return A function that will wait for the results
   */
  public <A, B> Function<A, B> joinAsync(Function<A, Future<B>> f, long timeoutMagnitude, TimeUnit timeoutUnits) {
    Objects.requireNonNull(f, "function to wrap");
    return a -> {
      try {
        return f.apply(a).get(timeoutMagnitude, timeoutUnits);
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