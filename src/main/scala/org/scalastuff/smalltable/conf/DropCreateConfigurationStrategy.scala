package org.scalastuff.smalltable
package conf

import me.prettyprint.hector.api.ddl.ColumnDefinition
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition
import me.prettyprint.hector.api.Cluster
import scalaz._
import Scalaz._
import me.prettyprint.hector.api.ddl.ColumnType
import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory

case class DropCreateConfigurationStrategy(
  requiredCfDef: ColumnFamilyDefinition,
  ksName: String) extends CheckConfigurationStrategy with CreateColumnFamily {
  
  val logger = LoggerFactory.getLogger(this.getClass)

  def onCFFound(existing: ColumnFamilyDefinition): ValidationNEL[String, Cluster => Cluster] = { cluster: Cluster =>
    val newCfDef = createCFDef()
    
    logger.warn("Column Family '%s' found in Keyspace '%s', re-creating it".format(requiredCfDef.getName, ksName))
    cluster.dropColumnFamily(ksName, requiredCfDef.getName, true)
    logger.info("Column Family '%s' successfuly dropped from Keyspace '%s', creating new one. Most settings will be preserved.".format(requiredCfDef.getName, ksName))
    cluster.addColumnFamily(newCfDef, true)
    logger.info("Column Family '%s' successfuly created in Keyspace '%s'".format(requiredCfDef.getName, ksName))
    
    cluster
  } success

  
}