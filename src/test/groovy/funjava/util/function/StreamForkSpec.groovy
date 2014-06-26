package funjava.util.function

import spock.lang.AutoCleanup
import spock.lang.Specification
import spock.lang.Subject

import java.util.function.UnaryOperator
import java.util.stream.Stream

class StreamForkSpec extends Specification {

  boolean upstreamIsClosed = false

  @AutoCleanup
  Stream<Integer> upstream = Stream.iterate(1, { i -> i + 1 } as UnaryOperator).onClose({ -> upstreamIsClosed = true })

  @Subject
  @AutoCleanup
  StreamFork<Integer> fork = new StreamFork<>(upstream)

  def "single-argument constructor explodes on null"() {
    when:
    new StreamFork<Object>(null)

    then:
    thrown(NullPointerException)
  }

  def "two-argument constructor explodes on null"() {
    when:
    new StreamFork<Object>(null, 1)

    then:
    thrown(NullPointerException)
  }

  def "two-argument constructor tolerates any value between -2 and 2"() {
    when:
    new StreamFork<>(Stream.of(1, 2, 3), (int) i)

    then:
    noExceptionThrown()

    where:
    i << [-2, -1, 0, 1, 2]
  }

  def "a single fork gives the right first value"() {
    when:
    Stream forked = fork.fork()

    then:
    forked.findFirst().get() == 1
  }

  def "forking twice gives the right first value for both forks"() {
    when:
    Stream forked1 = fork.fork()
    Stream forked2 = fork.fork()

    then:
    forked1.findFirst().get() == 1
    forked2.findFirst().get() == 1
  }

  def "when fork is closed, upstream is closed"() {
    when:
    fork.close()

    then:
    upstreamIsClosed
  }

  def "gives the correct elements to each of two forks"() {
    when:
    Stream<Integer> forked1 = fork.fork().limit(5)
    Stream<Integer> forked2 = fork.fork().limit(10)

    then:
    forked2.max(Comparator.naturalOrder()).get() == 10
    forked1.max(Comparator.naturalOrder()).get() == 5
  }

  def "forkCount constructor will capture values for a later fork"() {
    given:
    Stream<Integer> upstream = Stream.iterate(1, { i -> i + 1 } as UnaryOperator)
    StreamFork<Integer> fork = new StreamFork<>(upstream, 2)
    Stream<Integer> forked1 = fork.fork().limit(10)

    when:
    forked1.max(Comparator.naturalOrder()).get() // Read 10 elements from stream
    Stream<Integer> forked2 = fork.fork().limit(5)

    then:
    forked2.max(Comparator.naturalOrder()).get() == 5
  }

  def "fork greater than forkCount will not capture values for a later fork"() {
    given:
    Stream<Integer> upstream = Stream.iterate(1, { i -> i + 1 } as UnaryOperator)
    StreamFork<Integer> fork = new StreamFork<>(upstream, 1)
    Stream<Integer> forked1 = fork.fork().limit(10)

    when:
    forked1.max(Comparator.naturalOrder()).get() // Read 10 elements from stream
    Stream<Integer> forked2 = fork.fork().limit(5)

    then:
    forked2.max(Comparator.naturalOrder()).get() == 15 // 10 + 5
  }


}
