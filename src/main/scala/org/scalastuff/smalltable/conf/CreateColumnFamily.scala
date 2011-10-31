package org.scalastuff.smalltable
package conf

import org.slf4j.Logger
import me.prettyprint.cassandra.service.ThriftCfDef
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition
import me.prettyprint.hector.api.Cluster
import scalaz.Scalaz._

trait CreateColumnFamily {
  def requiredCfDef: ColumnFamilyDefinition
  def ksName: String  
  def logger: Logger

  def onCFNotFound(): ValidationNEL[String, Cluster => Cluster] = { cluster: Cluster =>
    val newCfDef = createCFDef()

    logger.warn("Column Family '%s' not found in Keyspace '%s', creating new one".format(newCfDef.getName, ksName))
    cluster.addColumnFamily(newCfDef, true)
    logger.info("Column Family '%s' successfuly created in Keyspace '%s'".format(newCfDef.getName, ksName))

    cluster
  } success
  
  def createCFDef() = {
    val newCfDef = new ThriftCfDef(requiredCfDef)
    newCfDef.setKeyspaceName(ksName)
    if (newCfDef.getComment == null)
      newCfDef.setComment("Auto-created by SmallTable")
      
    newCfDef
  }
}