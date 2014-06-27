package funjava.lang.reflect

import spock.lang.Specification
import spock.lang.Subject

import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.Wrapper

class WrapperInvocationHandlerSpec extends Specification {

  static interface WrappedInterface {}

  static class Wrapped implements WrappedInterface {}

  Wrapped wrapped = new Wrapped()

  @Subject
  WrapperInvocationHandler subject = new WrapperInvocationHandler(wrapped)

  def "created proxy implements Wrapper"() {
    when:
    def proxy = Proxy.newProxyInstance(Wrapped.classLoader, [Wrapper] as Class[], subject)

    then:
    proxy instanceof Wrapper
    Wrapper.isAssignableFrom(proxy.class)
  }

  def "unwrap using concrete class returns wrapped instance"() {
    given:
    Wrapper proxy = Proxy.newProxyInstance(Wrapped.classLoader, [Wrapper] as Class[], subject)

    when:
    def returned = proxy.unwrap(Wrapped)

    then:
    returned == wrapped
  }

  def "unwrap using interface returns wrapped instance"() {
    given:
    Wrapper proxy = Proxy.newProxyInstance(Wrapped.classLoader, [Wrapper, WrappedInterface] as Class[], subject)

    when:
    def returned = proxy.unwrap(WrappedInterface)

    then:
    returned == wrapped
  }

  def "isWrapperFor works with interface"() {
    given:
    Wrapper proxy = Proxy.newProxyInstance(Wrapped.classLoader, [Wrapper, WrappedInterface] as Class[], subject)

    when:
    boolean wrappedInterface = proxy.isWrapperFor(WrappedInterface)
    boolean wrappedConnection = proxy.isWrapperFor(Connection)

    then:
    wrappedInterface
    !wrappedConnection
  }

  def "isWrapperFor works with concrete instance"() {
    given:
    Wrapper proxy = Proxy.newProxyInstance(Wrapped.classLoader, [Wrapper, WrappedInterface] as Class[], subject)

    when:
    boolean wrappedConcrete = proxy.isWrapperFor(Wrapped)
    boolean wrappedArrayList = proxy.isWrapperFor(ArrayList)

    then:
    wrappedConcrete
    !wrappedArrayList
  }


}
