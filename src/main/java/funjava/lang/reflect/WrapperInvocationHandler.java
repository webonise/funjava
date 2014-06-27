package funjava.lang.reflect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.*;

/**
 * Implements an {@link java.lang.reflect.InvocationHandler} that handles {@link java.sql.Wrapper} method invocations.
 */
public class WrapperInvocationHandler<T> implements InvocationHandler {

  private final T wrapped;
  private final boolean wrappedIsWrapper;

  public WrapperInvocationHandler(final T wrapped) {
    Objects.requireNonNull(wrapped, "object to be wrapped");
    this.wrapped = wrapped;
    this.wrappedIsWrapper = this.wrapped instanceof Wrapper;
  }

  @Override
  public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
    if (args.length == 1 && Class.class.isAssignableFrom(args[0].getClass())) {
      Class<?> clazz = Class.class.cast(args[0]);
      String methodName = method.getName();
      switch (methodName) {
        case "unwrap":
          return unwrapTo(clazz);
        case "isWrapperFor":
          return isWrapped(clazz);
        default: // Fall through
      }
    }
    return method.invoke(wrapped, args);
  }

  private <A> A unwrapTo(Class<A> clazz) throws SQLException {
    Objects.requireNonNull(clazz, "class to unwrap to");
    if (wrappedIsWrapper) {
      Wrapper w = ((Wrapper) wrapped);
      if (w.isWrapperFor(clazz)) return w.unwrap(clazz);
    }
    if (clazz.isAssignableFrom(wrapped.getClass())) return clazz.cast(wrapped);
    throw new SQLException("Could not unwrap " + wrapped + " to " + clazz);
  }

  private boolean isWrapped(Class<?> clazz) throws SQLException {
    Objects.requireNonNull(clazz, "class to check for wrapping");
    if (clazz.isAssignableFrom(wrapped.getClass())) return true;
    if (wrappedIsWrapper) return ((Wrapper) wrapped).isWrapperFor(clazz);
    return false;
  }


}
