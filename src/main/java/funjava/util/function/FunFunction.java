package funjava.util.function;

import funjava.FunJava;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * An extension to the {@link java.util.function.Function} interface with various useful methods.
 */
public interface FunFunction<A, B> extends Function<A, B> {

  /**
   * Creates a {@code FunFunction} from a {@code Function}. If the function is a {@code FunFunction} already, just
   * casts it; otherwise, wraps the function.
   *
   * @param f   The function to convert; cannot be {@code null}.
   * @param <A> The argument type.
   * @param <B> The return type.
   * @return The appropriately typed function; never {@code null}.
   */
  public static <A, B> FunFunction<A, B> of(Function<A, B> f) {
    Objects.requireNonNull(f, "function to convert");
    if (f instanceof FunFunction) {
      return FunFunction.class.cast(f);
    } else {
      return f::apply;
    }
  }

  /**
   * Generate a consumer that takes the argument, calls this, and ignores the return value.
   *
   * @return A consumer that calls this function and ignores its return value.
   */
  public default FunConsumer<A> consumerise() {
    return a -> apply(a);
  }

  /**
   * Generate a supplier that, each time it is called, will pass the given argument to this and return the result.
   *
   * @return A supplier that calls this function with the argument each time
   */
  public default FunSupplier<B> supplierise(A a) {
    return () -> apply(a);
  }

  /**
   * Generate a supplier that, each time it is called,
   * will pass {@code null} to this and return the result.
   *
   * @return A supplier that calls this function with the argument each time
   */
  public default FunSupplier<B> supplierise() {
    return () -> apply(null);
  }

  /**
   * Return a function that calls this function asynchronously.
   *
   * @return A function that calls this function asynchronously
   * @see funjava.FunJava#getExecutor()
   */
  public default FunFunction<A, Future<B>> executeAsync() {
    return a -> FunJava.getExecutor().submit(() -> apply(a));
  }

  /**
   * Given a function, return a function that consumes {@link java.util.concurrent.Future} instances and produces
   * {@link
   * java.util.concurrent.Future} instances by applying the function to the result asynchronously.
   *
   * @return A function that consumes and produces futures.
   */
  public default FunFunction<Future<A>, Future<B>> asyncise() {
    // TODO This should delegate to a monadic future.
    return a -> FunJava.getExecutor().submit(() -> apply(a.get()));
  }

  /**
   * Given a function and a value, partially apply the value to the function.
   *
   * @see #supplierise(Object)
   */
  public default Supplier<B> applyPartial(A a) {
    return supplierise(a);
  }


  /**
   * Given a function, create a bifunction that ignores the first argument.
   *
   * @return A bifunction that calls {@code this}, ignoring the first argument and passing in the second argument.
   */
  public default <X> FunBiFunction<X, A, B> unapply() {
    return (a, b) -> apply(b);
  }

  /**
   * Given a test and this function, provides a function that will return the optional wrapper around {@code this(x)}
   * if the test is true; else, it will return {@link java.util.Optional#empty()}.
   *
   * @param test The test to see if the function should be applied; never {@code null}
   * @return A function mapping elements where the test is {@code true}; never {@code null}
   */
  public default FunFunction<A, Optional<B>> applyIf(Predicate<A> test) {
    Objects.requireNonNull(test, "test to be executed");
    return a -> {
      if (test.test(a)) {
        return Optional.of(apply(a));
      } else {
        return Optional.empty();
      }
    };
  }

  /**
   * Given a test and functions {@code this} and {@code g}, provides a function that will return {@code this(x)} if the
   * test is true, and {@code g(x)} if the test is false.
   *
   * @param test    The test to see which function should be applied; never {@code null}
   * @param ifFalse The function to execute if the test is false; never {@code null}
   * @return A function mapping elements based on the truth value of the test; never {@code null}
   */
  public default FunFunction<A, B> applyIfElse(Predicate<A> test, Function<A, B> ifFalse) {
    Objects.requireNonNull(test, "test to be executed");
    Objects.requireNonNull(ifFalse, "function to be applied when test is false");
    return a -> {
      Function<A, B> f = test.test(a) ? this : ifFalse;
      return f.apply(a);
    };
  }

}
