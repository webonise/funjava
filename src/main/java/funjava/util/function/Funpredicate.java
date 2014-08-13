package funjava.util.function;

import java.util.*;
import java.util.function.*;

public interface FunPredicate<T> extends Predicate<T> {


  /**
   * Provides a predicate that returns {@code true} if the predicate's argument is less than the given object.
   *
   * @param object The object which forms the exclusive upper bound; never {@code null}
   * @param <A>    The type to compare
   * @return A predicate that returns {@code true} iff its argument is less than {@code object}; never {@code null}.
   */
  public static <A extends Comparable> FunPredicate<A> isLessThan(A object) {
    Objects.requireNonNull(object, "object to compare against");
    return them -> object.compareTo(them) < 0;
  }

  /**
   * Provides a predicate that returns {@code true} if the predicate's argument is greater than the given object.
   *
   * @param object The object which forms the exclusive lower bound; never {@code null}
   * @param <A>    The type to compare
   * @return A predicate that returns {@code true} iff its argument is greater than {@code object}; never {@code null}.
   */
  public static <A extends Comparable> FunPredicate<A> isGreaterThan(A object) {
    Objects.requireNonNull(object, "object to compare against");
    return them -> object.compareTo(them) > 0;
  }

  /**
   * Provides a predicate that returns {@code true} if the predicate's argument is not greater than the given object.
   *
   * @param object The object which forms the inclusive upper bound; never {@code null}
   * @param <A>    The type to compare
   * @return A predicate that returns {@code true} iff its argument is not greater than {@code object}; never {@code
   * null}.
   */
  public static <A extends Comparable> FunPredicate<A> isLessThanOrEqual(A object) {
    Objects.requireNonNull(object, "object to compare against");
    return them -> object.compareTo(them) <= 0;
  }


  /**
   * Provides a predicate that returns {@code true} if the predicate's argument is not less than the given object.
   *
   * @param object The object which forms the inclusive lower bound; never {@code null}
   * @param <A>    The type to compare
   * @return A predicate that returns {@code true} iff its argument is not less than {@code object}; never {@code null}.
   */
  public static <A extends Comparable> FunPredicate<A> isGreaterThanOrEqual(A object) {
    Objects.requireNonNull(object, "object to compare against");
    return them -> object.compareTo(them) >= 0;
  }

  /**
   * Provides a predicate to determine if the argument is null.
   *
   * @param <A> The type to consider.
   * @return A predicate that returns {@code true} iff the argument is {@code null}; never {@code null}.
   */
  public static <A> FunPredicate<A> isNullPredicate() {
    return a -> a == null;
  }

  /**
   * Provides a predicate to determine if the argument is not null.
   *
   * @param <A> The type to consider.
   * @return A predicate that returns {@code true} iff the argument is not {@code null}; never {@code null}.
   */
  public static <A> FunPredicate<A> notNullPredicate() {
    return a -> a != null;
  }

}
