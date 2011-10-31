package org.scalastuff.smalltable
package conf

import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.Cluster
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition
import org.slf4j.LoggerFactory
import me.prettyprint.cassandra.service.ThriftColumnDef
import me.prettyprint.cassandra.model.BasicColumnDefinition
import me.prettyprint.hector.api.ddl.ColumnIndexType
import me.prettyprint.hector.api.ddl.ColumnDefinition

trait CheckConfigurationMode {
  def createCheckConfigurationStrategy(
    required: ColumnFamilyDefinitionEx[_],
    keyspaceName: String): CheckConfigurationStrategy

  def onKeyspaceNotFound(cluster: Cluster, keyspaceName: String)
}

object CheckConfigurationMode {
  /**
   * Validates if DB configuration matches required types.
   *
   * Exception is thrown otherwise.
   */
  val Validate = new CheckConfigurationMode {
    def createCheckConfigurationStrategy(required: ColumnFamilyDefinitionEx[_], keyspaceName: String) = ValidateConfigurationStrategy(required, keyspaceName)

    def onKeyspaceNotFound(cluster: Cluster, keyspaceName: String) {
      sys.error("Keyspace '%s' not found" format keyspaceName)
    }
  }

  /**
   * Updates types if they do not match, creates CFs if they do not exist.
   *
   * No data contained in CFs is updated.
   */
  def Update(replicationFactor: Int = 1, strategyClass: String = "org.apache.cassandra.locator.SimpleStrategy") =
    new CheckConfigurationMode {
      def createCheckConfigurationStrategy(
        required: ColumnFamilyDefinitionEx[_],
        keyspaceName: String) =
        UpdateConfigurationStrategy(required, keyspaceName)

      def onKeyspaceNotFound(cluster: Cluster, keyspaceName: String) {
        createKeyspace(cluster, keyspaceName, replicationFactor, strategyClass)
      }
    }

  /**
   * Drops CF and recreates it from scratch.
   *
   * Old data gets lost.
   */
  def DropCreate(replicationFactor: Int = 1, strategyClass: String = "org.apache.cassandra.locator.SimpleStrategy") =
    new CheckConfigurationMode {
      def createCheckConfigurationStrategy(
        required: ColumnFamilyDefinitionEx[_],
        keyspaceName: String) =
        DropCreateConfigurationStrategy(required, keyspaceName)

      def onKeyspaceNotFound(cluster: Cluster, keyspaceName: String) {
        createKeyspace(cluster, keyspaceName, replicationFactor, strategyClass)
      }
    }

  private[this] def createKeyspace(cluster: Cluster, keyspaceName: String, replicationFactor: Int, strategyClass: String) {
    val logger = LoggerFactory.getLogger("org.scalastuff.smalltable.Database")
    
    logger.warn("Keyspace {} not found, creating new one with RF={} and strategyClass={}", Array(keyspaceName, replicationFactor, strategyClass))
    val ksDef = HFactory.createKeyspaceDefinition(keyspaceName, strategyClass, replicationFactor, java.util.Collections.emptyList[ColumnFamilyDefinition]());
    cluster.addKeyspace(ksDef, true)
    logger.info("Keyspace successfuly created")
  }
}