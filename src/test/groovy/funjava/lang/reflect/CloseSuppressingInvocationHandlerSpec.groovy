package funjava.lang.reflect

import spock.lang.Specification
import spock.lang.Subject
import java.lang.reflect.Proxy


class CloseSuppressingInvocationHandlerSpec extends Specification {

  static class MyCloseable implements Closeable {
    boolean closed = false
    void close() { closed = true }
  }

  MyCloseable closeMe = new MyCloseable()
  @Subject CloseSuppressingInvocationHandler subject = new CloseSuppressingInvocationHandler(closeMe)

  void "sanity check that close works"() {
    when:
    closeMe.close()

    then:
    closeMe.closed
  }

  void "suppresses close"() {
    given:
    Closeable proxy = Proxy.newProxyInstance(MyCloseable.classLoader, [Closeable] as Class[], subject)

    when:
    proxy.close()

    then:
    !closeMe.closed
  }

}
