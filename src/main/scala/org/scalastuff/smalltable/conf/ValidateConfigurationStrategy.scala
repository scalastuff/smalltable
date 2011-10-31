package org.scalastuff.smalltable
package conf

import me.prettyprint.hector.api.ddl.ColumnDefinition
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition
import me.prettyprint.hector.api.Cluster
import scalaz._
import Scalaz._

case class ValidateConfigurationStrategy(
  requiredCfDef: ColumnFamilyDefinitionEx[_],
  ksName: String) extends DetailedCheckConfigurationStrategy(requiredCfDef) {

  def onCFNotFound(): ValidationNEL[String, Cluster => Cluster] = "Column Family not found".failNel

  def onWrongKeyValueType(existing: ColumnFamilyDefinition, required: String): ValidationNEL[String, Update[ColumnFamilyDefinition]] = {
    logger.warn("Column Family '%s' in Keyspace '%s' has key_validation_class=%s, expected: key_validation_class=%s".
      format(requiredCfDef.getName(), ksName, existing.getKeyValidationClass, requiredCfDef.getKeyValidationClass))
    passThroughUpdate[ColumnFamilyDefinition]
  }

  def onMissingColumn(column: ColumnDefinition): ValidationNEL[String, Check[ColumnDefinition]] = {
    logger.warn("Column Family '%s' in Keyspace '%s' doesn't have column '%s' configured".
      format(requiredCfDef.getName(), ksName, requiredCfDef.columnNameSerializer.fromByteBuffer(column.getName).toString))
    passThroughCheck(column)
  }

  def onUnknownColumn(column: ColumnDefinition): ValidationNEL[String, Check[ColumnDefinition]] = {
    logger.warn("Column Family '%s' in Keyspace '%s' has unknown column '%s'".
      format(requiredCfDef.getName(), ksName, requiredCfDef.columnNameSerializer.fromByteBuffer(column.getName).toString))
    passThroughCheck(column)
  }

  def onWrongColumnConfig(existing: ColumnDefinition, required: ColumnDefinition): ValidationNEL[String, Update[ColumnDefinition]] = {
    if (existing.getValidationClass != null) {
      "Column '%s' has validation_class='%s', validation_class='%s' expected".
        format(requiredCfDef.columnNameSerializer.fromByteBuffer(existing.getName).toString, existing.getValidationClass, required.getValidationClass)
        .failNel
    } else {
      logger.warn("Column Family '%s' in Keyspace '%s' has no validation_class configured for column '%s'".
        format(requiredCfDef.getName(), ksName, requiredCfDef.columnNameSerializer.fromByteBuffer(existing.getName).toString))
      passThroughUpdate[ColumnDefinition]
    }
  }
  
  def onWrongIndexConfig(existing: ColumnDefinition, required: ColumnDefinition): ValidationNEL[String, Update[ColumnDefinition]] = {
    if (existing.getIndexType() == null) {
      "Column '%s' expected to be indexed".
        format(requiredCfDef.columnNameSerializer.fromByteBuffer(existing.getName).toString)
        .failNel
    } else {
      logger.warn("Column Family '%s' in Keyspace '%s' has unused index on column '%s'".
        format(requiredCfDef.getName(), ksName, requiredCfDef.columnNameSerializer.fromByteBuffer(existing.getName).toString))
      passThroughUpdate[ColumnDefinition]
    }
  }
}