package org.scalastuff.smalltable

import scala.collection.JavaConversions._
import org.scalastuff.scalabeans.Preamble._
import me.prettyprint.hector.api.mutation.Mutator
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.Keyspace
import scala.annotation.implicitNotFound
import org.scalastuff.smalltable.mapper._
import org.scalastuff.smalltable.view._
import me.prettyprint.hector.api.Serializer
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition
import me.prettyprint.hector.api.ddl.ColumnType
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition
import org.scalastuff.smalltable.conf.ColumnFamilyDefinitionExImpl
import org.scalastuff.smalltable.conf.ColumnFamilyDefinitionEx

abstract class SuperColumnFamily(_name: String) extends ColumnOrSuperColumnFamily(_name) { scf =>
  type SuperColumnName

  def superColumnNameSerializer(implicit mf: Manifest[SuperColumnName]) = serializers.forType[SuperColumnName]

  def columnFamilyDefinition(implicit keyMf: Manifest[KeyValue], scnMf: Manifest[SuperColumnName], cnMf: Manifest[ColumnName]): ColumnFamilyDefinitionEx[ColumnName] = {
    val cfDef = new ColumnFamilyDefinitionExImpl[ColumnName](columnNameSerializer)
    cfDef.setName(name)
    cfDef.setColumnType(ColumnType.SUPER)
    cfDef.setComparatorType(superColumnNameSerializer.getComparatorType())
    cfDef.setSubComparatorType(columnNameSerializer.getComparatorType())
    cfDef.setKeyValidationClass(rowKeySerializer.getComparatorType().getClassName())
    columnDefinitions foreach { colDef => cfDef.addColumnDefinition(colDef) }
    cfDef
  }

  def materializedView[B <: AnyRef](implicit beanMf: Manifest[B], keyMf: Manifest[KeyValue], scnMf: Manifest[SuperColumnName], cnMf: Manifest[ColumnName]) =
    new MaterializedViewBuilder[B]()

  class MaterializedViewBuilder[B <: AnyRef]()(implicit beanMf: Manifest[B], keyMf: Manifest[KeyValue], scnMf: Manifest[SuperColumnName], cnMf: Manifest[ColumnName]) {
    def rowKeyProperty(rowKeyPropertyName: String) = {
      val bd = descriptorOf[B]
      new MaterializedViewBuilderD(PropertyMapper[B, KeyValue](bd(rowKeyPropertyName), rowKeySerializer))
    }

    def rowKey(rowKey: KeyValue) =
      new MaterializedViewBuilderS(new StaticMapper[B, KeyValue](rowKey, rowKeySerializer))
  }

  class MaterializedViewBuilderD[B <: AnyRef](keyField: Mapper[B, KeyValue])(implicit beanMf: Manifest[B],
                                                                             keyMf: Manifest[KeyValue],
                                                                             scnMf: Manifest[SuperColumnName],
                                                                             cnMf: Manifest[ColumnName]) {

    def superColumnNameProperty(superColumnNamePropertyName: String) = {
      val bd = descriptorOf[B]

      new MaterializedViewBuilderDD[B](
        keyField,
        PropertyMapper[B, SuperColumnName](bd(superColumnNamePropertyName), superColumnNameSerializer))
    }

    def superColumnName(superColumnName: SuperColumnName) = {
      new MaterializedViewBuilderDS[B](
        keyField,
        new StaticMapper[B, SuperColumnName](superColumnName, superColumnNameSerializer))
    }
  }

  class MaterializedViewBuilderS[B <: AnyRef](
    keyField: StaticMapper[B, KeyValue])(implicit beanMf: Manifest[B],
                                         keyMf: Manifest[KeyValue],
                                         scnMf: Manifest[SuperColumnName],
                                         cnMf: Manifest[ColumnName]) {

    def superColumnNameProperty(superColumnNamePropertyName: String) = {
      val bd = descriptorOf[B]

      new MaterializedViewBuilderSD[B](
        keyField,
        PropertyMapper[B, SuperColumnName](bd(superColumnNamePropertyName), superColumnNameSerializer))
    }

    def superColumnName(superColumnName: SuperColumnName) = {
      new MaterializedViewBuilderSS[B](
        keyField,
        new StaticMapper[B, SuperColumnName](superColumnName, superColumnNameSerializer))
    }
  }

  class MaterializedViewBuilderDD[B <: AnyRef](
    keyField: Mapper[B, KeyValue],
    superColumnNameField: Mapper[B, SuperColumnName])(implicit beanMf: Manifest[B],
                                                      keyMf: Manifest[KeyValue],
                                                      scnMf: Manifest[SuperColumnName],
                                                      cnMf: Manifest[ColumnName]) {

    @implicitNotFound(msg = "Static column names can be used only for the Column Families where ColumnName type is String")
    def staticColumnNames(exclude: Iterable[String] = Nil, rename: Iterable[(String, String)] = Nil)(implicit ev: ColumnName =:= String) = {
      val mapper = new OCsMapper[B](serializers, keyField.propertyNames ++ superColumnNameField.propertyNames ++ exclude, rename)
      addColumnDefinitions(mapper.columnDefinitions())
      new MaterializedViewSuperColumnDKDC[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, superColumnNameSerializer, mapper)
    }

    def columnNameProperty(subColumnNamePropertyName: String) = new {
      def valueProperty(valuePropertyName: String) = {
        val bd = descriptorOf[B]

        val columnMapper =
          new DynamicColumnMapper[B, ColumnName, Any](
            PropertyMapper[B, ColumnName](bd(subColumnNamePropertyName), columnNameSerializer),
            PropertyMapper[B](bd(valuePropertyName), serializers))

        new MaterializedViewSubColumnDDD[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, superColumnNameSerializer, columnMapper, columnNameSerializer)
      }

      def valueObject() = {
        val bd = descriptorOf[B]

        val columnMapper =
          new DynamicColumnMapper[B, ColumnName, Array[Byte]](
            PropertyMapper[B, ColumnName](bd(subColumnNamePropertyName), columnNameSerializer),
            new ObjectMapper[B](keyField.propertyNames ++ superColumnNameField.propertyNames))

        new MaterializedViewSubColumnDDD[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, superColumnNameSerializer, columnMapper, columnNameSerializer)
      }
    }

    def columnName(columnName: ColumnName) = new {
      def valueProperty(valuePropertyName: String) = {
        val bd = descriptorOf[B]

        val columnMapper =
          new StaticColumnMapper[B, ColumnName, Any](
            columnName,
            columnNameSerializer,
            PropertyMapper[B](bd(valuePropertyName), serializers))

        new MaterializedViewSubColumnDDS[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, superColumnNameSerializer, columnMapper)
      }

      def valueObject() = {
        val bd = descriptorOf[B]

        val columnMapper =
          new StaticColumnMapper[B, ColumnName, Array[Byte]](
            columnName,
            columnNameSerializer,
            new ObjectMapper[B](keyField.propertyNames ++ superColumnNameField.propertyNames))

        new MaterializedViewSubColumnDDS[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, superColumnNameSerializer, columnMapper)
      }
    }
  }

  class MaterializedViewBuilderSD[B <: AnyRef](
    keyField: StaticMapper[B, KeyValue],
    superColumnNameField: Mapper[B, SuperColumnName])(implicit beanMf: Manifest[B],
                                                      keyMf: Manifest[KeyValue],
                                                      scnMf: Manifest[SuperColumnName],
                                                      cnMf: Manifest[ColumnName]) {

    @implicitNotFound(msg = "Static column names can be used only for the Column Families where ColumnName type is String")
    def staticColumnNames(exclude: Iterable[String] = Nil, rename: Iterable[(String, String)] = Nil)(implicit ev: ColumnName =:= String) = {
      val mapper = new OCsMapper[B](serializers, keyField.propertyNames ++ superColumnNameField.propertyNames ++ exclude, rename)
      addColumnDefinitions(mapper.columnDefinitions())
      new MaterializedViewSuperColumnSKDC[B, scf.type](scf, keyField, superColumnNameField, superColumnNameSerializer, mapper)
    }

    def columnNameProperty(subColumnNamePropertyName: String) = new {
      def valueProperty(valuePropertyName: String) = {
        val bd = descriptorOf[B]

        val columnMapper =
          new DynamicColumnMapper[B, ColumnName, Any](
            PropertyMapper[B, ColumnName](bd(subColumnNamePropertyName), columnNameSerializer),
            PropertyMapper[B](bd(valuePropertyName), serializers))

        new MaterializedViewSubColumnSDD[B, scf.type](scf, keyField, superColumnNameField, superColumnNameSerializer, columnMapper, columnNameSerializer)
      }

      def valueObject() = {
        val bd = descriptorOf[B]

        val columnMapper =
          new DynamicColumnMapper[B, ColumnName, Array[Byte]](
            PropertyMapper[B, ColumnName](bd(subColumnNamePropertyName), columnNameSerializer),
            new ObjectMapper[B](keyField.propertyNames ++ superColumnNameField.propertyNames))

        new MaterializedViewSubColumnSDD[B, scf.type](scf, keyField, superColumnNameField, superColumnNameSerializer, columnMapper, columnNameSerializer)
      }
    }

    def columnName(columnName: ColumnName) = new {
      def valueProperty(valuePropertyName: String) = {
        val bd = descriptorOf[B]

        val columnMapper =
          new StaticColumnMapper[B, ColumnName, Any](
            columnName,
            columnNameSerializer,
            PropertyMapper[B](bd(valuePropertyName), serializers))

        new MaterializedViewSubColumnSDS[B, scf.type](scf, keyField, superColumnNameField, superColumnNameSerializer, columnMapper)
      }

      def valueObject() = {
        val bd = descriptorOf[B]

        val columnMapper =
          new StaticColumnMapper[B, ColumnName, Array[Byte]](
            columnName,
            columnNameSerializer,
            new ObjectMapper[B](keyField.propertyNames ++ superColumnNameField.propertyNames))

        new MaterializedViewSubColumnSDS[B, scf.type](scf, keyField, superColumnNameField, superColumnNameSerializer, columnMapper)
      }
    }
  }

  class MaterializedViewBuilderDS[B <: AnyRef](
    keyField: Mapper[B, KeyValue],
    superColumnNameField: StaticMapper[B, SuperColumnName])(implicit beanMf: Manifest[B],
                                                            keyMf: Manifest[KeyValue],
                                                            scnMf: Manifest[SuperColumnName],
                                                            cnMf: Manifest[ColumnName]) {

    @implicitNotFound(msg = "Static column names can be used only for the Column Families where ColumnName type is String")
    def staticColumnNames(exclude: Iterable[String] = Nil, rename: Iterable[(String, String)] = Nil)(implicit ev: ColumnName =:= String) = {
      val mapper = new OCsMapper[B](serializers, keyField.propertyNames ++ superColumnNameField.propertyNames ++ exclude, rename)
      addColumnDefinitions(mapper.columnDefinitions())
      new MaterializedViewSuperColumnDKSC[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, mapper)
    }

    def columnNameProperty(subColumnNamePropertyName: String) = new {
      def valueProperty(valuePropertyName: String) = {
        val bd = descriptorOf[B]

        val columnMapper =
          new DynamicColumnMapper[B, ColumnName, Any](
            PropertyMapper[B, ColumnName](bd(subColumnNamePropertyName), columnNameSerializer),
            PropertyMapper[B](bd(valuePropertyName), serializers))

        new MaterializedViewSubColumnDSD[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, columnMapper, columnNameSerializer)
      }

      def valueObject() = {
        val bd = descriptorOf[B]

        val columnMapper =
          new DynamicColumnMapper[B, ColumnName, Array[Byte]](
            PropertyMapper[B, ColumnName](bd(subColumnNamePropertyName), columnNameSerializer),
            new ObjectMapper[B](keyField.propertyNames ++ superColumnNameField.propertyNames))

        new MaterializedViewSubColumnDSD[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, columnMapper, columnNameSerializer)
      }
    }

    def columnName(columnName: ColumnName) = new {
      def valueProperty(valuePropertyName: String) = {
        val bd = descriptorOf[B]

        val columnMapper =
          new StaticColumnMapper[B, ColumnName, Any](
            columnName,
            columnNameSerializer,
            PropertyMapper[B](bd(valuePropertyName), serializers))

        new MaterializedViewSubColumnDSS[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, columnMapper)
      }

      def valueObject() = {
        val bd = descriptorOf[B]

        val columnMapper =
          new StaticColumnMapper[B, ColumnName, Array[Byte]](
            columnName,
            columnNameSerializer,
            new ObjectMapper[B](keyField.propertyNames ++ superColumnNameField.propertyNames))

        new MaterializedViewSubColumnDSS[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, columnMapper)
      }
    }
  }

  class MaterializedViewBuilderSS[B <: AnyRef](
    keyField: StaticMapper[B, KeyValue],
    superColumnNameField: StaticMapper[B, SuperColumnName])(implicit beanMf: Manifest[B],
                                                            keyMf: Manifest[KeyValue],
                                                            scnMf: Manifest[SuperColumnName],
                                                            cnMf: Manifest[ColumnName]) {

    @implicitNotFound(msg = "Static column names can be used only for the Column Families where ColumnName type is String")
    def staticColumnNames(exclude: Iterable[String] = Nil, rename: Iterable[(String, String)] = Nil)(implicit ev: ColumnName =:= String) = {
      val mapper = new OCsMapper[B](serializers, keyField.propertyNames ++ superColumnNameField.propertyNames ++ exclude, rename)
      addColumnDefinitions(mapper.columnDefinitions())
      new MaterializedViewSuperColumnSKSC[B, scf.type](scf, keyField, superColumnNameField, mapper)
    }

    def columnNameProperty(subColumnNamePropertyName: String) = new {
      def valueProperty(valuePropertyName: String) = {
        val bd = descriptorOf[B]

        val columnMapper =
          new DynamicColumnMapper[B, ColumnName, Any](
            PropertyMapper[B, ColumnName](bd(subColumnNamePropertyName), columnNameSerializer),
            PropertyMapper[B](bd(valuePropertyName), serializers))

        new MaterializedViewSubColumnSSD[B, scf.type](scf, keyField, superColumnNameField, columnMapper, columnNameSerializer)
      }

      def valueObject() = {
        val bd = descriptorOf[B]

        val columnMapper =
          new DynamicColumnMapper[B, ColumnName, Array[Byte]](
            PropertyMapper[B, ColumnName](bd(subColumnNamePropertyName), columnNameSerializer),
            new ObjectMapper[B](keyField.propertyNames ++ superColumnNameField.propertyNames))

        new MaterializedViewSubColumnSSD[B, scf.type](scf, keyField, superColumnNameField, columnMapper, columnNameSerializer)
      }
    }

    def columnName(columnName: ColumnName) = new {
      def valueProperty(valuePropertyName: String) = {
        val bd = descriptorOf[B]

        val columnMapper =
          new StaticColumnMapper[B, ColumnName, Any](
            columnName,
            columnNameSerializer,
            PropertyMapper[B](bd(valuePropertyName), serializers))

        new MaterializedViewSubColumnSSS[B, scf.type](scf, keyField, superColumnNameField, columnMapper)
      }

      def valueObject() = {
        val bd = descriptorOf[B]

        val columnMapper =
          new StaticColumnMapper[B, ColumnName, Array[Byte]](
            columnName,
            columnNameSerializer,
            new ObjectMapper[B](keyField.propertyNames ++ superColumnNameField.propertyNames))

        new MaterializedViewSubColumnSSS[B, scf.type](scf, keyField, superColumnNameField, columnMapper)
      }
    }
  }
}