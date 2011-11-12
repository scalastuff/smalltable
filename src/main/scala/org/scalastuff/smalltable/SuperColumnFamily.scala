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

      val superColumnNameField = PropertyMapper[B, SuperColumnName](bd(superColumnNamePropertyName), superColumnNameSerializer)
      new MaterializedSCFViewBuilderCV(
        keyField,
        superColumnNameField,
        new MaterializedViewSuperColumnDKDC[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, superColumnNameSerializer, _: OCsMapper[B]),
        new MaterializedViewSubColumnDDD[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, superColumnNameSerializer, _: OCMapper[B, _, _], columnNameSerializer),
        new MaterializedViewSubColumnDDS[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, superColumnNameSerializer, _: StaticColumnMapper[B, _, _])
      )
    }

    def superColumnName(superColumnName: SuperColumnName) = {
      val superColumnNameField = new StaticMapper[B, SuperColumnName](superColumnName, superColumnNameSerializer)
      new MaterializedSCFViewBuilderCV(
        keyField,
        superColumnNameField,
        new MaterializedViewSuperColumnDKSC[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, _: OCsMapper[B]),
        new MaterializedViewSubColumnDSD[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, _: OCMapper[B, _, _], columnNameSerializer),
        new MaterializedViewSubColumnDSS[B, scf.type](scf, keyField, rowKeySerializer, superColumnNameField, _: StaticColumnMapper[B, _, _])
      )
    }
  }

  class MaterializedViewBuilderS[B <: AnyRef](
    keyField: StaticMapper[B, KeyValue])(implicit beanMf: Manifest[B],
                                         keyMf: Manifest[KeyValue],
                                         scnMf: Manifest[SuperColumnName],
                                         cnMf: Manifest[ColumnName]) {

    def superColumnNameProperty(superColumnNamePropertyName: String) = {
      val bd = descriptorOf[B]

      val superColumnNameField = PropertyMapper[B, SuperColumnName](bd(superColumnNamePropertyName), superColumnNameSerializer)
      new MaterializedSCFViewBuilderCV(
        keyField,
        superColumnNameField,
        new MaterializedViewSuperColumnSKDC[B, scf.type](scf, keyField, superColumnNameField, superColumnNameSerializer, _: OCsMapper[B]),
        new MaterializedViewSubColumnSDD[B, scf.type](scf, keyField, superColumnNameField, superColumnNameSerializer, _: OCMapper[B, _, _], columnNameSerializer),
        new MaterializedViewSubColumnSDS[B, scf.type](scf, keyField, superColumnNameField, superColumnNameSerializer, _: StaticColumnMapper[B, _, _])
      )
    }

    def superColumnName(superColumnName: SuperColumnName) = {
      val superColumnNameField = new StaticMapper[B, SuperColumnName](superColumnName, superColumnNameSerializer)
      new MaterializedSCFViewBuilderCV(
        keyField,
        superColumnNameField,
        new MaterializedViewSuperColumnSKSC[B, scf.type](scf, keyField, superColumnNameField, _: OCsMapper[B]),
        new MaterializedViewSubColumnSSD[B, scf.type](scf, keyField, superColumnNameField, _: OCMapper[B, _, _], columnNameSerializer),
        new MaterializedViewSubColumnSSS[B, scf.type](scf, keyField, superColumnNameField, _: StaticColumnMapper[B, _, _])
      )
    }
  }

  class MaterializedSCFViewBuilderCV[B <: AnyRef, SCNSV, DCNV, SCNV](
    keyField: Mapper[B, KeyValue],
    superColumnNameField: Mapper[B, SuperColumnName],
    createStaticColumnNamesView: OCsMapper[B] => SCNSV,
    createDynamicColumnNameView: OCMapper[B, _, _] => DCNV,
    createStaticColumnNameView: StaticColumnMapper[B, _, _] => SCNV)(implicit beanMf: Manifest[B], columnNameMf: Manifest[ColumnName])
    extends MaterializedViewBuilderCV[B, ColumnName, DCNV, SCNV](
      keyField.propertyNames ++ superColumnNameField.propertyNames,
      serializers,
      columnNameSerializer,
      createDynamicColumnNameView,
      createStaticColumnNameView) {

    @implicitNotFound(msg = "Static column names can be used only for the Super Column Families where ColumnName type is String")
    def staticColumnNames(exclude: Iterable[String] = Nil, rename: Iterable[(String, String)] = Nil)(implicit ev: ColumnName =:= String) = {
      val mapper = new OCsMapper[B](serializers, keyField.propertyNames ++ superColumnNameField.propertyNames ++ exclude, rename)
      addColumnDefinitions(mapper.columnDefinitions())
      createStaticColumnNamesView(mapper)
    }
  }
}