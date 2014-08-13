package funjava.lang.reflect

import spock.lang.Specification
import spock.lang.Subject

import java.lang.reflect.Proxy
import java.sql.Connection

class CloseSuppressingInvocationHandlerSpec extends Specification {

  static class MyCloseable implements Closeable {
    boolean closed = false

    void close() { closed = true }
  }

  MyCloseable closeMe = new MyCloseable()
  @Subject
  CloseSuppressingInvocationHandler subject = new CloseSuppressingInvocationHandler(closeMe)

  void "sanity check that close works"() {
    when:
    closeMe.close()

    then:
    closeMe.closed
  }

  void "suppresses close"() {
    given:
    Connection proxy = Proxy.newProxyInstance(MyCloseable.classLoader, [Connection] as Class[], subject)

    when:
    proxy.close()

    then:
    proxy.isClosed()
    !closeMe.closed
  }

  void "isClosed returns false if close has not been called"() {
    given:
    Connection proxy = Proxy.newProxyInstance(MyCloseable.classLoader, [Connection] as Class[], subject)

    expect:
    !proxy.isClosed()
  }

}
