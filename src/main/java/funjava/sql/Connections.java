package funjava.sql;

import funjava.lang.reflect.CloseSuppressingInvocationHandler;
import funjava.lang.reflect.WrapperInvocationHandler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Wrapper;
import java.util.*;

/**
 * Utilities for working with a {@link java.sql.Connection}.
 */
public class Connections {

  /**
   * Creates a proxy that suppresses calls to {@link java.sql.Connection#close()}, and otherwise
   * passes all the method calls along to the underlying class.
   *
   * @param conn The connection to proxy; never {@code null}
   * @return A proxy that will suppress calls to {@code close}
   */
  public static Connection createSuppressingCloseProxy(Connection conn) {
    Objects.requireNonNull(conn, "connection to proxy");
    return createProxy(new CloseSuppressingInvocationHandler(conn));
  }

  /**
   * Given a {@link java.lang.reflect.InvocationHandler}, generate a proxy for a {@link java.sql.Connection}.
   * This performs the actual {@link java.lang.reflect.Proxy} logic, along with wrapping the proxy in a
   * {@link funjava.lang.reflect.WrapperInvocationHandler} proxy.
   *
   * @param handler the handler for connection method calls.
   * @return A {@link java.sql.Connection} proxy that delegates to the handler; never {@code null}
   */
  public static Connection createProxy(InvocationHandler handler) {
    Objects.requireNonNull(handler, "the handler for connection method calls");
    ClassLoader classLoader = handler.getClass().getClassLoader();
    Object proxy = Proxy.newProxyInstance(classLoader, new Class[]{Connection.class}, handler);
    proxy = Proxy.newProxyInstance(
                                      classLoader,
                                      new Class[]{Connection.class, Wrapper.class},
                                      new WrapperInvocationHandler(proxy)
    );
    Connection proxyConn = Connection.class.cast(proxy);
    return proxyConn;
  }


}
