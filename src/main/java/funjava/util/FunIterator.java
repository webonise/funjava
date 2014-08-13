package funjava.util;


import java.util.*;

/**
 * An implementation of {@link java.util.Iterator} that can be implemented as a lambda. You can also use this class as
 * a simpler way to implement an {@code Iterator}.
 * <p>
 * This interface does not support {@link #remove()} out of the box.
 */
public interface FunIterator<E> extends Iterator<E> {

  /**
   * Provides the next element, optionally consuming it. This is the abstract method to implement as a lambda.
   * If {@code false} is passed in as an argument, this method should always return the same value, and should never
   * exhaust the iterator. If {@code true} is passed in as an argument, the iterator should advance to the next element
   * after returning the value. At any point in time, if there are no more elements, the iterator should return
   * {@link java.util.Optional#empty()}. This method should never throw an exception or return {@code null}.
   *
   * @param consume  Whether the element should be consumed after being returned.
   * @return {@link java.util.Optional#empty()}  if there are no other elements; otherwise, an {@link
   * java.util.Optional} containing the next element. Never {@code null}.
   */
  Optional<E> nextElement(boolean consume);

  @Override
  default boolean hasNext() {
    return nextElement(false).isPresent();
  }

  @Override
  default E next() {
    return nextElement(true).orElseThrow(() -> new NoSuchElementException("Iterator is exhausted"));
  }

}
