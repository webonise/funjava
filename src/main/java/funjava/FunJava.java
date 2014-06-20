package funjava;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Infrastructure and basic utilities used throughout the project.
 */
public class FunJava {

  private static volatile ExecutorService executor;
  static {
    final AtomicLong ctr = new AtomicLong(0L);
    Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        Logger logger = Logger.getLogger(FunJava.class.getName());
        logger.log(Level.SEVERE, "Uncaught exception in executor", e);
      }
    };
    ThreadFactory factory = new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(FunJava.class.getSimpleName() + " #" + ctr.incrementAndGet());
        t.setUncaughtExceptionHandler(handler);
        t.setDaemon(true);
        t.setPriority(Thread.NORM_PRIORITY);
        return t;
      }
    };
    executor = Executors.newCachedThreadPool(factory);
  }

  /**
   * Provides the executor that the system will use.
   *
   * @return The executor that the system will use by default.
   * @throws java.lang.IllegalStateException If the executor is shutdown.
   */
  public static ExecutorService getExecutor() {
    synchronized (executor) {
      if (executor.isShutdown()) {
        throw new IllegalStateException("Executor is shutdown");
      }
      return executor;
    }
  }

  /**
   * Assigns the executor that the system will use.
   *
   * @param executor The executor to use; never {@code null}.
   * @throws java.lang.IllegalArgumentException If the executor is shutdown.
   */
  public static void setExecutor(ExecutorService executor) {
    Objects.requireNonNull(executor, "Executor");
    if (executor.isShutdown()) {
      throw new IllegalArgumentException("Executor is shutdown");
    }
    FunJava.executor = executor;
  }
}
