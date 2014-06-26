package funjava;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * Infrastructure and basic utilities used throughout the project.
 */
public class FunJava {

  private static final AtomicLong ctr = new AtomicLong(0L);
  private static volatile ExecutorService executor = createDefaultExecutorService();

  // Shutdown the executor when we shutdown the runtime: this will trigger exceptions in any running tasks.
  static {
    Thread t = new Thread(() -> getExecutor().shutdownNow());
    t.setPriority(Thread.MIN_PRIORITY); // Hint that higher priority threads should be allowed to execute
    Runtime.getRuntime().addShutdownHook(t);
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
        executor = createDefaultExecutorService();
      }
      return executor;
    }
  }

  /**
   * Provides an instance of the default-configured executor service.
   *
   * @return A new executor service to be used; never {@code null}.
   */
  public static ExecutorService createDefaultExecutorService() {
    Thread.UncaughtExceptionHandler handler = (t, e) -> {
      Logger logger = Logger.getLogger(FunJava.class.getName());
      logger.log(Level.SEVERE, "Uncaught exception in executor", e);
    };
    ThreadFactory factory = r -> {
      Thread t = new Thread(r);
      t.setName(FunJava.class.getSimpleName() + " #" + Long.toHexString(ctr.incrementAndGet()));
      t.setUncaughtExceptionHandler(handler);
      t.setDaemon(true);
      t.setPriority(Thread.MIN_PRIORITY);
      return t;
    };
    return Executors.newCachedThreadPool(factory);
  }

  /**
   * Assigns the executor that the system will use.
   *
   * @param executor The executor to use; never {@code null}.
   * @throws java.lang.IllegalArgumentException If the executor is shutdown.
   */
  public static void setExecutor(ExecutorService executor) {
    Objects.requireNonNull(executor, "Executor");
    FunJava.executor = executor;
  }
}
