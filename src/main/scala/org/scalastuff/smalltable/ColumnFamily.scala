package org.scalastuff.smalltable

import java.util.concurrent.ConcurrentHashMap
import scala.annotation.implicitNotFound
import scala.collection.JavaConversions._

import org.scalastuff.scalabeans.Preamble._
import me.prettyprint.cassandra.model.BasicColumnDefinition
import me.prettyprint.cassandra.service.ThriftColumnDef
import me.prettyprint.hector.api.ddl.ColumnDefinition
import me.prettyprint.hector.api.ddl.ColumnIndexType
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import java.util.Arrays
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition
import me.prettyprint.cassandra.service.ThriftCfDef
import me.prettyprint.hector.api.ddl.ColumnType
import me.prettyprint.hector.api.Serializer
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition
import org.scalastuff.smalltable.conf.ColumnFamilyDefinitionEx
import org.scalastuff.smalltable.conf.ColumnFamilyDefinitionExImpl
import org.scalastuff.smalltable.serde.StandardSerializers
import org.scalastuff.scalabeans.BeanDescriptor
import org.scalastuff.scalabeans.PropertyDescriptor
import org.scalastuff.smalltable.view._
import org.scalastuff.smalltable.mapper._


abstract class ColumnOrSuperColumnFamily(val name: String) {
  type KeyValue
  type ColumnName
  
  def serializers = StandardSerializers.registry
  def rowKeySerializer(implicit mf: Manifest[KeyValue]) = serializers.forType[KeyValue]
  def columnNameSerializer(implicit mf: Manifest[ColumnName]) = serializers.forType[ColumnName]

  protected[this] def columnDefinitions: Seq[ColumnDefinition] = _columnDefinitions.toSeq
  private[this] val _columnDefinitions = ArrayBuffer[ColumnDefinition]()
  protected[this] def addColumnDefinitions(newColDefs: Iterable[ColumnDefinition])(implicit ev: Manifest[ColumnName]) = {
    for (newColumnDefinition <- newColDefs) {
      _columnDefinitions.find(_.getName() == newColumnDefinition.getName()) match {
        case Some(existing) =>
          // duplicate column definition found, check if columns are the same
          if (existing.getValidationClass() == newColumnDefinition.getValidationClass()) {
            if (existing.getIndexType() == null && newColumnDefinition != null) {
              _columnDefinitions.remove(existing)
              _columnDefinitions.add(newColumnDefinition)
            }
          } else {
            throw new RuntimeException("ColumnFamily %s has at least 2 definitions of column '%s' with different types: %s and %s".
              format(name, columnNameSerializer.fromBytes(existing.getName.array()), existing.getValidationClass(), newColumnDefinition.getValidationClass())
            )
          }
        case None =>
          _columnDefinitions.add(newColumnDefinition)
      }
    }
  }
}

abstract class ColumnFamily(_name: String) extends ColumnOrSuperColumnFamily(_name) { cf =>

  def columnFamilyDefinition(implicit keyMf: Manifest[KeyValue], cnMf: Manifest[ColumnName]): ColumnFamilyDefinitionEx[ColumnName] = {
    val cfDef = new ColumnFamilyDefinitionExImpl[ColumnName](columnNameSerializer)
    cfDef.setName(name)    
    cfDef.setColumnType(ColumnType.STANDARD)
    cfDef.setComparatorType(columnNameSerializer.getComparatorType())
    cfDef.setKeyValidationClass(rowKeySerializer.getComparatorType().getClassName())
    columnDefinitions foreach { colDef => cfDef.addColumnDefinition(colDef) }
    cfDef
  }
  
  def materializedView[B <: AnyRef]()(implicit beanMf: Manifest[B], keyMf: Manifest[KeyValue], cnMf: Manifest[ColumnName]) = new MaterializedViewBuilder[B]()

  class MaterializedViewBuilder[B <: AnyRef](implicit beanMf: Manifest[B], keyMf: Manifest[KeyValue], cnMf: Manifest[ColumnName]) {
    private[this] val bd = descriptorOf[B]

    def rowKeyProperty(rowKeyPropertyName: String) = 
      new MaterializedViewBuilderDK(PropertyMapper[B, KeyValue](bd(rowKeyPropertyName), rowKeySerializer))
    
    def rowKey(rowKey: KeyValue) = 
      new MaterializedViewBuilderSK(new StaticMapper[B, KeyValue](rowKey, rowKeySerializer))
  }

  class MaterializedViewBuilderDK[B <: AnyRef](keyField: PropertyMapper[B, KeyValue])(implicit beanMf: Manifest[B], keyMf: Manifest[KeyValue], cnMf: Manifest[ColumnName]) {
    /**
     * @param index indexed *column names* (not properties)
     */
    @implicitNotFound(msg = "Static column names can be used only for the Column Families where ColumnName type is String")
    def staticColumnNames(
      exclude: Iterable[String] = Nil,
      rename: Iterable[(String, String)] = Nil,
      index: Iterable[String] = Nil)(implicit ev: ColumnName =:= String) = {
      val mapper = new OCsMapper[B](serializers, keyField.propertyNames ++ exclude, rename, index)
      addColumnDefinitions(mapper.columnDefinitions())
      new MaterializedViewRowDK[B, cf.type](cf, keyField, mapper, rowKeySerializer)
    }

    def columnNameProperty(columnNameProperty: String) = new {
      def valueProperty(valuePropertyName: String) = {
        val bd = descriptorOf[B]
        val valueProperty = bd(valuePropertyName)

        val columnMapper =
          new DynamicColumnMapper[B, ColumnName, Any](
            PropertyMapper[B, ColumnName](bd(columnNameProperty), columnNameSerializer),
            PropertyMapper[B](valueProperty, serializers))

        new MaterializedViewColumnDKDC[B, cf.type](cf, keyField, columnMapper, rowKeySerializer, columnNameSerializer)
      }

      def valueObject() = {
        val bd = descriptorOf[B]

        val columnMapper =
          new DynamicColumnMapper[B, ColumnName, Array[Byte]](
            PropertyMapper[B, ColumnName](bd(columnNameProperty), columnNameSerializer),
            new ObjectMapper[B](keyField.propertyNames))

        new MaterializedViewColumnDKDC[B, cf.type](cf, keyField, columnMapper, rowKeySerializer, columnNameSerializer)
      }
    }

    def columnName(columnName: ColumnName) = new {
      def valueProperty(valuePropertyName: String) = {
        val bd = descriptorOf[B]
        val valueProperty = bd(valuePropertyName)

        val columnMapper =
          new StaticColumnMapper[B, ColumnName, Any](
            columnName,
            columnNameSerializer,
            PropertyMapper[B](valueProperty, serializers))

        new MaterializedViewColumnDKSC[B, cf.type](cf, keyField, columnMapper, rowKeySerializer)
      }

      def valueObject() = {
        val columnMapper =
          new StaticColumnMapper[B, ColumnName, Array[Byte]](
            columnName,
            columnNameSerializer,
            new ObjectMapper[B](keyField.propertyNames))

        new MaterializedViewColumnDKSC[B, cf.type](cf, keyField, columnMapper, rowKeySerializer)
      }
    }
  }

  class MaterializedViewBuilderSK[B <: AnyRef](keyField: StaticMapper[B, KeyValue])(implicit beanMf: Manifest[B], cnMf: Manifest[ColumnName]) {
    @implicitNotFound(msg = "Static column names can be used only for the Column Families where ColumnName type is String")
    def staticColumnNames(
      exclude: Iterable[String] = Nil,
      rename: Iterable[(String, String)] = Nil,
      index: Iterable[String] = Nil)(implicit ev: ColumnName =:= String) = {
      val mapper = new OCsMapper[B](serializers, keyField.propertyNames ++ exclude, rename, index)
      addColumnDefinitions(mapper.columnDefinitions())
      new MaterializedViewRowSK[B, cf.type](cf, keyField, mapper)
    }

    def columnNameProperty(columnNameProperty: String) = new {
      def valueProperty(valuePropertyName: String) = {
        val bd = descriptorOf[B]
        val valueProperty = bd(valuePropertyName)

        val columnMapper =
          new DynamicColumnMapper[B, ColumnName, Any](
            PropertyMapper[B, ColumnName](bd(columnNameProperty), columnNameSerializer),
            PropertyMapper[B](valueProperty, serializers))

        new MaterializedViewColumnSKDC[B, cf.type](cf, keyField, columnMapper, columnNameSerializer)
      }

      def valueObject() = {
        val bd = descriptorOf[B]

        val columnMapper =
          new DynamicColumnMapper[B, ColumnName, Array[Byte]](
            PropertyMapper[B, ColumnName](bd(columnNameProperty), columnNameSerializer),
            new ObjectMapper[B](keyField.propertyNames))

        new MaterializedViewColumnSKDC[B, cf.type](cf, keyField, columnMapper, columnNameSerializer)
      }
    }

    def columnName(columnName: ColumnName) = new {
      def valueProperty(valuePropertyName: String) = {
        val bd = descriptorOf[B]
        val valueProperty = bd(valuePropertyName)

        val columnMapper =
          new StaticColumnMapper[B, ColumnName, Any](
            columnName,
            columnNameSerializer,
            PropertyMapper[B](valueProperty, serializers))

        new MaterializedViewColumnSKSC[B, cf.type](cf, keyField, columnMapper)
      }

      def valueObject() = new {
        val columnMapper =
          new StaticColumnMapper[B, ColumnName, Array[Byte]](
            columnName,
            columnNameSerializer,
            new ObjectMapper[B](keyField.propertyNames))

        new MaterializedViewColumnSKSC[B, cf.type](cf, keyField, columnMapper)
      }
    }
  }
}

