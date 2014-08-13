package funjava.lang.reflect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;

/**
 * An {@link java.lang.reflect.InvocationHandler} that suppresses calls to methods name "close" with no arguments, and
 * responds to methods named "{@code isClosed}" with no arguments by returning a {@code boolean} which is {@code true}
 * iff the aforementioned "{@code close}" method was called.
 */
public class CloseSuppressingInvocationHandler implements InvocationHandler {

  private final Object closeable;
  private volatile boolean isClosed = false;

  public CloseSuppressingInvocationHandler(Object closeable) {
    Objects.requireNonNull(closeable, "object to wrap");
    this.closeable = closeable;
  }

  /**
   * If the {@code method} is named {@code "close"}, return {@code null}. Otherwise, invoke {@code method}
   * on {@code proxy} using {@code args}.
   */
  @Override
  public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
    if (method == null) return null;
    final boolean noArgs = args == null || args.length == 0;
    if (noArgs && method.getName().equals("close")) {
      isClosed = true;
      return null;
    }
    if (noArgs && method.getName().equals("isClosed")) {
      return isClosed;
    }
    return method.invoke(closeable, args);
  }
}
