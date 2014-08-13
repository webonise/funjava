package funjava.sql

import java.sql.Connection

class ConnectionsSpec extends DatabaseDrivenSpecBase {

  def "close suppressing proxy does not immediately close"() {
    given:
    Connection proxy = Connections.createSuppressingCloseProxy(connection)

    expect:
    !proxy.isClosed()
    !connection.isClosed()
  }

  def "close suppressing proxy suppresses close"() {
    given:
    Connection proxy = Connections.createSuppressingCloseProxy(connection)

    when:
    proxy.close()

    then:
    proxy.isClosed()
    !connection.isClosed()
  }

  def "close suppressing proxy unwraps to the original connection"() {
    given:
    Connection proxy = Connections.createSuppressingCloseProxy(connection)

    when:
    Connection unwrapped = proxy.unwrap(Connection)

    then:
    connection != proxy
    connection == unwrapped
  }

}
