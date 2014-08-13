package funjava.util.function;

import funjava.FunJava;

import java.util.*;
import java.util.function.*;

/**
 * An extension to the {@link java.util.function.Consumer} interface with various useful methods.
 */
public interface FunConsumer<T> extends Consumer<T> {

  public static <T> FunConsumer<T> of(Consumer<T> consumer) {
    Objects.requireNonNull(consumer, "consumer to convert");
    if (consumer instanceof FunConsumer) {
      return FunConsumer.class.cast(consumer);
    } else {
      return consumer::accept;
    }
  }

  /**
   * Provides a consumer that does nothing.
   *
   * @param <T> The type to return.
   * @return A {@link java.util.function.Consumer} that does nothing.
   */
  public static <T> FunConsumer<T> doNothingConsumer() {
    return t -> {};
  }

  /**
   * Given a {@link java.util.function.Supplier}, construct a function that
   * consumes the argument using this and supplies the result using the supplier.
   *
   * @param s The supplier function; never {@code null}.
   * @return A function that passes the argument to the consumer and calls the supplier to generate the return value.
   */
  public default <B> FunFunction<T, B> functionise(Supplier<B> s) {
    Objects.requireNonNull(s, "supplier function");
    return a -> {
      accept(a);
      return s.get();
    };
  }

  /**
   * Convert this consumer to a function that consumes the argument and returns
   * {@link java.util.Optional#empty()}.
   *
   * @return A function that consumes the argument and returns the empty option.
   */
  public default <A> FunFunction<T, Optional<A>> functionise() {
    return a -> {
      accept(a);
      return Optional.empty();
    };
  }

  /**
   * Convert this consumer to a function that consumes the argument and returns the
   * provided value.
   *
   * @param returnValue The value to return.
   * @return A function that consumes the argument and returns the return value.
   */
  public default <B> FunFunction<T, B> functionise(B returnValue) {
    return a -> {
      accept(a);
      return returnValue;
    };
  }


  /**
   * Given a consumer, returns a consumer that consumes the result asynchronously.
   *
   * @return A consumer that calls the nested consumer asynchronously
   */
  public default Consumer<T> consumeAsync() {
    return a -> FunJava.getExecutor().submit(() -> this.accept(a));
  }

  /**
   * Given a function {@code f(x) -> y} and a consumer {@code g(y) -> ()}, return the consumer
   * {@code g(f(x)) -> ()}. This can be thought of as applying a transformat
   *
   * @param function The function to compose onto the consumer; never {@code null}.
   * @return A consumer that first passes its argument through the given function; never {@code null}
   */
  public default <U> FunConsumer<U> compose(Function<U, ? extends T> function) {
    Objects.requireNonNull(function, "the function to compose");
    return a -> accept(function.apply(a));
  }

  /**
   * Performs the task of {@code consumer} iff {@code test} returns {@code true}.
   *
   * @param test The test to see if the consumer should be applied; never {@code null}
   * @return A consumer that will consume the element only if the test is true; never {@code null}
   */
  public default Consumer<T> consumeIf(Predicate<T> test) {
    Objects.requireNonNull(test, "test to perform");
    return a -> {
      if (test.test(a)) accept(a);
    };
  }


}
