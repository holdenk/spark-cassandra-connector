package com.datastax.spark.connector.cql

import com.datastax.driver.core.{AuthProvider, PlainTextAuthProvider, Cluster}
import com.datastax.driver.core.Cluster.Builder
import org.apache.cassandra.thrift.{TFramedTransportFactory, ITransportFactory, AuthenticationRequest, Cassandra}
import org.apache.cassandra.thrift.Cassandra.Iface
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._

/** Configures both native and Thrift connections to Cassandra.
  * This driver provides implementations [[NoAuthConfigurator]] for no authentication
  * and [[PasswordAuthConfigurator]] for password authentication. Other
  * configurators can be plugged in by setting `spark.cassandra.connection.conf.factory.class`
  * option. See [[ConnectionConfiguratorFactory]]. */
trait ConnectionConfigurator extends Serializable {

  /** Sets appropriate connection options for native connections. */
  def configureClusterBuilder(builder: Cluster.Builder): Cluster.Builder
  
  /** Returns the transport factory for creating thrift transports.
    * Some authentication mechanisms like SASL require custom thrift transport.
    * To be removed in the near future, when the dependency from Thrift will be completely dropped. */
  def transportFactory: ITransportFactory

  /** Sets appropriate authentication options for Thrift connection.
    * To be removed in the near future, when the dependency from Thrift will be completely dropped. */
  def configureThriftClient(client: Cassandra.Iface)

}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
case object NoAuthConfigurator extends ConnectionConfigurator {
  override def transportFactory = new TFramedTransportFactory
  override def configureThriftClient(client: Iface) {}

  override def configureClusterBuilder(builder: Builder): Builder = {
    builder.withAuthProvider(AuthProvider.NONE)
  }
}

/** Performs plain-text password authentication. Use with `PasswordAuthenticator` in Cassandra. */
case class PasswordAuthConfigurator(user: String, password: String) extends ConnectionConfigurator {
  override def transportFactory = new TFramedTransportFactory

  override def configureThriftClient(client: Iface) = {
    val authRequest = new AuthenticationRequest(Map("username" -> user, "password" -> password))
    client.login(authRequest)
  }

  override def configureClusterBuilder(builder: Builder): Builder = {
    builder.withAuthProvider(new PlainTextAuthProvider(user, password))
  }
}

/** Obtains a connection configurator by reading  `SparkConf` object. */
trait ConnectionConfiguratorFactory {
  def configurator(conf: SparkConf): ConnectionConfigurator
}

/** Default `ConnectionConfiguratorFactory` that supports no authentication or password authentication.
  * Password authentication is enabled when both `spark.cassandra.auth.username` and `spark.cassandra.auth.password`
  * options are present in `SparkConf`.*/
class DefaultConnConfFactory extends ConnectionConfiguratorFactory {

  val CassandraUserNameProperty = "spark.cassandra.auth.username"
  val CassandraPasswordProperty = "spark.cassandra.auth.password"

  def configurator(conf: SparkConf): ConnectionConfigurator = {
    val credentials = 
      for (username <- conf.getOption(CassandraUserNameProperty);
           password <- conf.getOption(CassandraPasswordProperty)) yield (username, password)

    credentials match {
      case Some((user, password)) => PasswordAuthConfigurator(user, password)
      case None => NoAuthConfigurator
    }
  }
}

/** Entry point for obtaining `ConnectionConfigurator` object from `SparkConf`, used when establishing connections to Cassandra.
  * The actual `ConnectionConfigurator` creation is delegated to the [[ConnectionConfiguratorFactory]] pointed by
  * `spark.cassandra.auth.conf.factory.class` property. */
object ConnectionConfigurator {
  val ConnConfFactoryProperty = "spark.cassandra.connection.conf.factory.class"

  def fromSparkConf(conf: SparkConf) = {
    val connConfFactoryClass = conf.get(ConnConfFactoryProperty, classOf[DefaultConnConfFactory].getName)
    val connConfFactory = Class.forName(connConfFactoryClass).newInstance().asInstanceOf[ConnectionConfiguratorFactory]
    connConfFactory.configurator(conf)
  }
}
