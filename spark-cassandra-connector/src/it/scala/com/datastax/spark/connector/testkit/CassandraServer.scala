package com.datastax.spark.connector.testkit

import java.net.InetAddress

import com.datastax.spark.connector.cql.CassandraConnector

/** A utility trait for integration testing.
  * Manages *one* single Cassandra server at a time and enables switching its configuration.
  * This is not thread safe, and test suites must not be run in parallel,
  * because they will "steal" the server.*/
trait CassandraServer {
  def useCassandraConfig(configTemplate: String) {
    CassandraServer.useCassandraConfig(configTemplate)
  }
  def cassandraHost =
    CassandraServer.cassandraHost
}

object CassandraServer {
  private val HostProperty = "IT_TEST_CASSANDRA_HOST"

  private var cassandra: CassandraServerRunner = null
  private var currentConfigTemplate: String = null

  val cassandraHost = {
    val host = Option(System.getenv(CassandraServer.HostProperty)).getOrElse(DefaultHost)
    InetAddress.getByName(host)
  }

  /** Switches the Cassandra server to use the new configuration if the requested configuration is different
    * than the currently used configuration. When the configuration is switched, all the state (including data) of
    * the previously running cassandra cluster is lost.
    * @param configTemplate name of the cassandra.yaml template resource
    * @param forceReload if set to true, the server will be reloaded fresh even if the configuration didn't change */
  def useCassandraConfig(configTemplate: String, forceReload: Boolean = false) {
    if (System.getenv(HostProperty) == null) {
      if (currentConfigTemplate != configTemplate || forceReload) {
        CassandraConnector.evictCache()
        if (cassandra != null)
          cassandra.destroy()
        cassandra = new CassandraServerRunner(configTemplate)
        currentConfigTemplate = configTemplate
      }
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run() =
      if (cassandra != null)
        cassandra.destroy()
  }))

}
