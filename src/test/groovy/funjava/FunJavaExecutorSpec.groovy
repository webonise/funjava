package funjava

import spock.lang.Specification

class FunJavaExecutorSpec extends Specification {

  def "creates an executor"() {
    expect:
    FunJava.executor
    !FunJava.executor.shutdown
  }

  def "will create a new executor if old one is shutdown"() {
    when:
    FunJava.executor.shutdownNow()

    then:
    FunJava.executor
    !FunJava.executor.shutdown
  }

  def "will not accept a null executor"() {
    when:
    FunJava.executor = null

    then:
    thrown(NullPointerException)
  }


}
