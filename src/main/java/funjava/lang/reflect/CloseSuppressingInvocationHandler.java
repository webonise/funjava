package funjava.lang.reflect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;

/**
 * An {@link java.lang.reflect.InvocationHandler} that suppresses calls to methods name "closed".
 */
public class CloseSuppressingInvocationHandler implements InvocationHandler {

  private final Object closeable;

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
    if (method.getName().equals("close")) return null;
    return method.invoke(closeable, args);
  }
}
