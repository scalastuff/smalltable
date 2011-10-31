package org.scalastuff.smalltable
package conf

import me.prettyprint.hector.api.ddl.ColumnDefinition
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition
import me.prettyprint.hector.api.Cluster
import scalaz._
import Scalaz._
import me.prettyprint.hector.api.ddl.ColumnType
import scala.collection.JavaConversions._
import me.prettyprint.cassandra.service.ThriftCfDef
import me.prettyprint.cassandra.model.BasicColumnDefinition
import me.prettyprint.cassandra.service.ThriftColumnDef

case class UpdateConfigurationStrategy(
  requiredCfDef: ColumnFamilyDefinitionEx[_],
  ksName: String) extends DetailedCheckConfigurationStrategy(requiredCfDef) with CreateColumnFamily {
  
  private[this] def columnName(column: ColumnDefinition) = requiredCfDef.columnNameSerializer.fromByteBuffer(column.getName.duplicate())

  def onWrongKeyValueType(existing: ColumnFamilyDefinition, requiredKeyValueType: String): ValidationNEL[String, Update[ColumnFamilyDefinition]] = { updated: ColumnFamilyDefinition =>
    val newUpdated = new ThriftCfDef(updated)
    newUpdated.setKeyValidationClass(requiredKeyValueType)

    Some((newUpdated, nel("key_validation_class=%s (was %s)".format(requiredKeyValueType, existing.getKeyValidationClass))))
  } success

  def onMissingColumn(column: ColumnDefinition): ValidationNEL[String, Check[ColumnDefinition]] = {
    Check(Some(column), Vector("Add column '%s', set validation_class=%s"
      .format(columnName(column), column.getValidationClass))).success
  }

  def onUnknownColumn(column: ColumnDefinition): ValidationNEL[String, Check[ColumnDefinition]] = {
    Check(None, Vector("Remove column '%s'".format(columnName(column)))).success
  }

  def onWrongColumnConfig(existing: ColumnDefinition, required: ColumnDefinition): ValidationNEL[String, Update[ColumnDefinition]] = { updated: ColumnDefinition =>
    Some((required, nel("Update column '%s', set validation_class=%s (was %s)"
      .format(columnName(required), required.getValidationClass, existing.getValidationClass))))
  } success
  
  def onWrongIndexConfig(existing: ColumnDefinition, required: ColumnDefinition): ValidationNEL[String, Update[ColumnDefinition]] = { updated: ColumnDefinition =>
   Some((required, nel("Update column '%s', set index_type=%s (was %s)"
      .format(columnName(required), required.getIndexType, existing.getIndexType))))
  } success
}