package org.scalastuff.smalltable.conf
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition
import me.prettyprint.hector.api.Serializer
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition

trait ColumnFamilyDefinitionEx[CN] extends ColumnFamilyDefinition {
  def columnNameSerializer: Serializer[CN]
}

class ColumnFamilyDefinitionExImpl[CN](val columnNameSerializer: Serializer[CN]) extends BasicColumnFamilyDefinition with ColumnFamilyDefinitionEx[CN] 