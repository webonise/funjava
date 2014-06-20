package funjava.util.concurrent;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * Provides an {@link java.util.Iterator} API for consuming a {@link java.util.concurrent.BlockingQueue} being filled by
 * a {@link java.util.concurrent.Future}. This class is very useful, given a few limitations: <ul> <li>This class
 * presumes that it is the only thing drawing from the queue.</li> <li>This class is NOT itself thread-safe.</li>
 * <li>The queue cannot contain {@code null} elements as meaningful values.</li> </ul>
 * <p>
 * Note that this class's {@link #forEachRemaining(java.util.function.Consumer)} is much more efficient than the classic
 * {@link java.util.Iterator} loop.
 */
public class DrawingIterator<T> implements Iterator<T> {

  private final Future<?> task;
  private final BlockingQueue<? extends T> queue;
  private volatile T next = null;

  /**
   * Constructs an instance.
   *
   * @param fillingTask The task responsible for filling the queue; never {@code null}
   * @param queue       The queue being filled; never {@code null}
   */
  public DrawingIterator(Future<?> fillingTask, BlockingQueue<? extends T> queue) {
    Objects.requireNonNull(fillingTask, "task responsible for filling the queue");
    Objects.requireNonNull(queue, "queue that is being filled");
    this.task = fillingTask;
    this.queue = queue;
  }

  /**
   * The task we are relying on to populate the queue.
   *
   * @return The task that should populate the queue; never {@code null}.
   */
  public Future<?> getTask() {
    return task;
  }

  /**
   * See if we have another item in the queue. If necessary, this method will cause the current thread to wait for the
   * queue to be filled or for the task to resolve.
   */
  @Override
  public boolean hasNext() {
    if (next != null) return true;
    if (!queue.isEmpty()) return true;
    if (task.isDone() && queue.isEmpty()) return false;

    // First, try to draw from the queue.
    try {
      // There's a possible race condition where we're at the end: the task has finished before we enter the test below.
      // If that's the case, this timeout is all wasted time, so keep the time window very small.
      next = queue.poll(10L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted while waiting on next item", e);
    }

    // If that didn't work, the task has gotten slow in filling the queue.
    // Give the task a chance to resolve, and generally cool our heels for a bit.
    if (next == null) {
      try {
        // If the task is done, this will return immediately. Otherwise, wait for it to either be done or to get a good
        // head start on us.
        task.get(100L, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new IllegalStateException("Interrupted while waiting on next item", e);
      } catch (ExecutionException e) {
        throw new IllegalStateException("Task filling the queue threw an exception", e);
      } catch (TimeoutException e) {
        Thread.yield(); // Take a beat and move onto the next round
      }
    }

    // Tail recurse to see where we're at
    return hasNext();
  }

  @Override
  public T next() {
    if (next != null) {
      T toReturn = next;
      next = null;
      return toReturn;
    }

    // Since we can assume that we have a next element here, just blindly take an element from the queue.
    try {
      return queue.take();
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted while trying to draw from queue", e);
    } finally {
      // Give the other thread a chance to do its job and stay ahead of us
      if (!task.isDone()) Thread.yield();
    }
  }

  @Override
  public void forEachRemaining(final Consumer<? super T> action) {
    // First, check if we're done
    if (task.isDone() && queue.isEmpty()) return;

    // Keep draining the queue until it's really empty
    List<T> drain = new ArrayList<>();
    while (!queue.isEmpty()) {
      queue.drainTo(drain);
      drain.forEach(action::accept);
      drain.clear();
    }

    // We got ahead of the task; give it a chance to get ahead of us
    // (It could also be that we're done, in which case what follows is an expensive nop.)
    try {
      // If the task is done, this will return immediately
      task.get(10L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for task to fill the queue", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Task filling the queue threw an exception", e);
    } catch (TimeoutException e) {
      // Task is still alive; that's fine—move on
    }

    // Tail recurse and let the next round determine if we're done
    forEachRemaining(action);
  }

}
