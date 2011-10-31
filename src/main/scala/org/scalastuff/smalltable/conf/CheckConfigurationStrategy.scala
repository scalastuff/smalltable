package org.scalastuff.smalltable.conf

import scala.collection.JavaConversions._

import org.slf4j.LoggerFactory

import me.prettyprint.cassandra.service.ThriftCfDef
import me.prettyprint.hector.api.ddl.ColumnDefinition
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition
import me.prettyprint.hector.api.ddl.ColumnType
import me.prettyprint.hector.api.Cluster
import scalaz.Scalaz._
import scalaz._

trait CheckConfigurationStrategy {
  def onCFNotFound(): ValidationNEL[String, Cluster => Cluster]
  def onCFFound(existing: ColumnFamilyDefinition): ValidationNEL[String, Cluster => Cluster]
}

abstract class DetailedCheckConfigurationStrategy(requiredCfDef: ColumnFamilyDefinitionEx[_]) extends CheckConfigurationStrategy {
  val logger = LoggerFactory.getLogger(this.getClass)

  //
  // Validation Utils
  //
  type Update[A] = A => Option[(A, NonEmptyList[String])]
  def accumulate[A](validation: ValidationNEL[String, Update[A]]) = {
    for (update <- validation) yield { existing: (A, Vector[String]) =>
      val (existingA, existingUpdateLog) = existing
      update(existingA) match {
        case None                        => existing
        case Some((updatedA, updateLog)) => (updatedA, existingUpdateLog ++ updateLog.list)
      }
    }
  }
  def passThroughUpdate[A] = { _: A => None }.success

  case class Check[A](checked: Option[A], log: Vector[String])
  def accumulateCheck[A](validation: ValidationNEL[String, Check[A]]) =
    for (check <- validation) yield { validated: (Vector[A], Vector[String]) =>
      val (existingAs, existingUpdateLog) = validated

      (existingAs ++ check.checked.toList, existingUpdateLog ++ check.log)
    }
  def passThroughCheck[A](a: A) = Check(Some(a), Vector.empty[String]).success

  //
  // ColumnFamily checks
  //    
  def onWrongKeyValueType(existing: ColumnFamilyDefinition, required: String): ValidationNEL[String, Update[ColumnFamilyDefinition]]

  def onCFFound(existing: ColumnFamilyDefinition): ValidationNEL[String, Cluster => Cluster] = {
    val columnFamilyTypeValidation =
      if (existing.getColumnType != requiredCfDef.getColumnType())
        "'%s' column type found, '%s' is expected".format(existing.getColumnType.getValue, requiredCfDef.getColumnType.getValue).failNel
      else
        passThroughUpdate[ColumnFamilyDefinition]

    val columNameTypeValidation =
      if (existing.getComparatorType != null && existing.getComparatorType.getTypeName != requiredCfDef.getComparatorType().getTypeName())
        "compare_with=%s found, compare_with=%s is expected".
          format(existing.getComparatorType.getTypeName, requiredCfDef.getComparatorType().getTypeName()).failNel
      else
        passThroughUpdate[ColumnFamilyDefinition]
    
    val subColumNameTypeValidation =
      if (existing.getSubComparatorType() != null && existing.getSubComparatorType.getTypeName != requiredCfDef.getSubComparatorType().getTypeName())
        "sub_compare_with=%s found, sub_compare_with=%s is expected".
          format(existing.getSubComparatorType.getTypeName, requiredCfDef.getSubComparatorType().getTypeName()).failNel
      else
        passThroughUpdate[ColumnFamilyDefinition]

    val keyValueTypeValidation =
      if (existing.getKeyValidationClass() != requiredCfDef.getKeyValidationClass())
        onWrongKeyValueType(existing, requiredCfDef.getKeyValidationClass())
      else
        passThroughUpdate[ColumnFamilyDefinition]

    val columnsValidation =
      for (updates <- validateColumns(existing.getColumnMetadata() sortBy (_.getName), requiredCfDef.getColumnMetadata().toSeq sortBy (_.getName))) yield {
        val (updatedColumnDefs, updateLog) = updates

        { existing: ColumnFamilyDefinition =>
          if (!updateLog.isEmpty) {
            val newCfDef = new ThriftCfDef(existing)
            newCfDef.setColumnMetadata(updatedColumnDefs)
            Some((newCfDef, nel(updateLog.head, updateLog.tail: _*)))
          } else None
        }
      }

    val cfValidation = ((existing, Vector.empty[String]) success) <*>
      accumulate(columnFamilyTypeValidation) <*>
      accumulate(columNameTypeValidation) <*>
      accumulate(subColumNameTypeValidation) <*>
      accumulate(keyValueTypeValidation) <*>
      accumulate(columnsValidation)

    for (updates <- cfValidation) yield { cluster =>
      val (updatedCfDef, updateLog) = updates
      if (!updateLog.isEmpty) {
        logger.info("About to update ColumnFamily '{}' in Keyspace '{}' with following changes:", existing.getName(), existing.getKeyspaceName())
        updateLog foreach { logLine => logger.info(logLine) }
        cluster.updateColumnFamily(updatedCfDef, true)
        logger.info("All updates are applied")
      }
      cluster
    }
  }

  //
  // Columns checks
  // 
  def onMissingColumn(column: ColumnDefinition): ValidationNEL[String, Check[ColumnDefinition]]
  def onUnknownColumn(column: ColumnDefinition): ValidationNEL[String, Check[ColumnDefinition]]
  def onWrongColumnConfig(existing:ColumnDefinition, required: ColumnDefinition): ValidationNEL[String, Update[ColumnDefinition]]
  def onWrongIndexConfig(existing:ColumnDefinition, required: ColumnDefinition): ValidationNEL[String, Update[ColumnDefinition]]
  def onRightColumnConfig(column: ColumnDefinition): ValidationNEL[String, Check[ColumnDefinition]] = passThroughCheck(column)

  /**
   * @param existing column definitions sorted by column name
   * @param required column definitions sorted by column name
   */
  def validateColumns(
    existing: Iterable[ColumnDefinition],
    required: Iterable[ColumnDefinition],
    validated: ValidationNEL[String, (Vector[ColumnDefinition], Vector[String])] = (Vector.empty[ColumnDefinition], Vector.empty) success): ValidationNEL[String, (Vector[ColumnDefinition], Vector[String])] = {

    val nextExisting = existing.headOption
    val nextRequired = required.headOption

    (nextExisting, nextRequired) match {
      case (None, None)    => validated
      case (None, Some(_)) => required.foldl(validated) { (validated, col) => validated <*> accumulateCheck(onMissingColumn(col)) }
      case (Some(_), None) => existing.foldl(validated) { (validated, col) => validated <*> accumulateCheck(onUnknownColumn(col)) }
      case (Some(existingColumn), Some(requiredColumn)) =>
        val compare = existingColumn.getName.compareTo(requiredColumn.getName)
        if (compare > 0)
          validateColumns(existing, required.tail, validated <*> accumulateCheck(onMissingColumn(requiredColumn)))
        else if (compare < 0)
          validateColumns(existing.tail, required, validated <*> accumulateCheck(onUnknownColumn(existingColumn)))
        else {
          val columnValueTypeValidation =
            if (existingColumn.getValidationClass != requiredColumn.getValidationClass)
              onWrongColumnConfig(existingColumn, requiredColumn)
            else
              passThroughUpdate[ColumnDefinition]

          val columnIndexValidation =
            if (existingColumn.getIndexType != requiredColumn.getIndexType)
              onWrongIndexConfig(existingColumn, requiredColumn)
            else
              passThroughUpdate[ColumnDefinition]

          val columnConfigValidation = ((existingColumn, Vector.empty[String]).success) <*>
          	accumulate(columnValueTypeValidation) <*>
          	accumulate(columnIndexValidation)

          validateColumns(existing.tail, required.tail, validated <*> accumulateCheck(columnConfigValidation.map(update => Check(Some(update._1), update._2))))
        }
    }
  }
}