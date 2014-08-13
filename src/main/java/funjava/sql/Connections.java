package funjava.sql;

import funjava.lang.reflect.CloseSuppressingInvocationHandler;
import funjava.lang.reflect.WrapperInvocationHandler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Wrapper;
import java.util.*;
import java.util.function.*;

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
    return createProxy(conn, CloseSuppressingInvocationHandler::new);
  }

  /**
   * Given a {@link java.lang.reflect.InvocationHandler}, generate a proxy for a {@link java.sql.Connection}.
   * This performs the actual {@link java.lang.reflect.Proxy} logic, along with wrapping the proxy in a
   * {@link funjava.lang.reflect.WrapperInvocationHandler} proxy.
   *
   * @param connection     the connection to proxy
   * @param handlerFactory the function that will generate a handler for connection method calls.
   * @return A {@link java.sql.Connection} proxy that delegates to the handler; never {@code null}
   */
  public static Connection createProxy(Connection connection, Function<Connection, InvocationHandler> handlerFactory) {
    Objects.requireNonNull(connection, "the connection to create");
    Objects.requireNonNull(handlerFactory, "the handler for connection method calls");
    ClassLoader classLoader = connection.getClass().getClassLoader();
    Connection proxy = Connection.class.cast(Proxy.newProxyInstance(
                                                                       classLoader,
                                                                       new Class[]{Connection.class, Wrapper.class},
                                                                       new WrapperInvocationHandler(connection)
        )
    );
    proxy = Connection.class.cast(Proxy.newProxyInstance(
                                                            classLoader,
                                                            new Class[]{Connection.class, Wrapper.class},
                                                            handlerFactory.apply(proxy)
        )
    );
    return proxy;
  }

}
