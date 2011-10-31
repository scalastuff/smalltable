package org.scalastuff.smalltable.view

import scala.annotation.implicitNotFound
import scala.collection.JavaConversions._

import org.scalastuff.smalltable.mapper._
import org.scalastuff.smalltable.SuperColumnFamily
import org.scalastuff.smalltable.serde.StandardSerializers
import org.scalastuff.scalabeans.Preamble.descriptorOf

import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.beans.HSuperColumn
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.mutation.Mutator
import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.Serializer

/**
 * Maps Subcolumn (Column of SuperColumn) to a bean
 */
private[view] class MaterializedViewSubColumn[B <: AnyRef, K, SCN, CN](
  scfName: String,
  keyField: Mapper[B, K],
  keySer: Serializer[K],
  superColumnNameField: Mapper[B, SCN],
  scnSer: Serializer[SCN],
  columnMapper: OCMapper[B, _, _],
  cnSer: Serializer[CN])(implicit beanMf: Manifest[B]) {

  def put(bean: B) = UpdateQuery(keySer, { mutator: Mutator[K] =>
    val rowKey = keyField.toValue(bean)
    val superColumName = superColumnNameField.toValue(bean)
    val superColumn = HFactory.createSuperColumn(
      superColumName,
      Seq(columnMapper.toColumn(bean).asInstanceOf[HColumn[Array[Byte], Array[Byte]]]),
      scnSer, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer)
    mutator.addInsertion(rowKey, scfName, superColumn)
  })

  def delete(rowKey: K, superColumnName: SCN, columnName: CN) = UpdateQuery(keySer, { mutator: Mutator[K] =>
    mutator.addSubDelete(rowKey, scfName, superColumnName, columnName, scnSer, cnSer)
  })

  def get(rowKey: K, superColumnName: SCN, columnName: CN) = { keyspace: Keyspace =>
    val qry = HFactory.createSubColumnQuery(keyspace, keySer, scnSer, cnSer, StandardSerializers.bytesSerializer)
    val result = qry.setColumnFamily(scfName).
      setKey(rowKey).
      setSuperColumn(superColumnName).
      setColumn(columnName).
      execute().
      get()

    if (result eq null) None
    else {
      val beanBuilder = bd.newBuilder()
      keyField.toBeanBuilder(rowKey, beanBuilder)
      superColumnNameField.toBeanBuilder(superColumnName, beanBuilder)
      columnMapper.toBeanBuilder(result, beanBuilder)
      Some(beanBuilder.result().asInstanceOf[B])
    }
  }

  def getSuperSlice(rowKey: K, superColumnName: SCN, superColumnNames: SCN*) = { keyspace: Keyspace =>
    val qry = HFactory.createSuperSliceQuery(keyspace, keySer, scnSer, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer)
    val result = qry.setColumnFamily(scfName).
      setKey(rowKey).
      setColumnNames((superColumnName +: superColumnNames): _*).
      execute().
      get().getSuperColumns()

    mapToBeans(rowKey, result)
  }

  def getSuperSlice(
    rowKey: K,
    from: Option[SCN] = None,
    to: Option[SCN] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) = { keyspace: Keyspace =>
    val qry = HFactory.createSuperSliceQuery(keyspace, keySer, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer)
    val result = qry.setColumnFamily(scfName).
      setKey(rowKey).
      setRange(
        from.map(from => scnSer.toBytes(from)) getOrElse NULL_BYTES,
        to.map(to => scnSer.toBytes(to)) getOrElse NULL_BYTES,
        reversed,
        count).
        execute().
        get().getSuperColumns()

    mapToBeans(rowKey, result)
  }

  def getSuperSliceSubColumn(rowKey: K, columnName: CN, superColumnName: SCN, superColumnNames: SCN*) = { keyspace: Keyspace =>
    val qry = HFactory.createSuperSliceQuery(keyspace, keySer, scnSer, cnSer, StandardSerializers.bytesSerializer)
    val result = qry.setColumnFamily(scfName).
      setKey(rowKey).
      setColumnNames((superColumnName +: superColumnNames): _*).
      execute().
      get().getSuperColumns()

    mapToBeans(rowKey, result, columnName)
  }

  def getSuperSliceSubColumn(
    rowKey: K,
    columnName: CN,
    from: Option[SCN] = None,
    to: Option[SCN] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) = { keyspace: Keyspace =>
    val qry = HFactory.createSuperSliceQuery(keyspace, keySer, StandardSerializers.bytesSerializer, cnSer, StandardSerializers.bytesSerializer)
    val result = qry.setColumnFamily(scfName).
      setKey(rowKey).
      setRange(
        from.map(from => scnSer.toBytes(from)) getOrElse NULL_BYTES,
        to.map(to => scnSer.toBytes(to)) getOrElse NULL_BYTES,
        reversed,
        count).
        execute().
        get().getSuperColumns()

    mapToBeans(rowKey, result, columnName)
  }

  def getSubSlice(rowKey: K, superColumnName: SCN, columnName: CN, columnNames: CN*) = { keyspace: Keyspace =>
    val qry = HFactory.createSubSliceQuery(keyspace, keySer, scnSer, cnSer, StandardSerializers.bytesSerializer)
    val result = qry.setColumnFamily(scfName).
      setKey(rowKey).
      setSuperColumn(superColumnName).
      setColumnNames((columnName +: columnNames): _*).
      execute().
      get().getColumns()

    mapToBeans(rowKey, superColumnName, result)
  }

  def getSubSlice(
    rowKey: K,
    superColumnName: SCN,
    from: Option[CN] = None,
    to: Option[CN] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) = { keyspace: Keyspace =>
    val qry = HFactory.createSubSliceQuery(keyspace, keySer, scnSer, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer)
    val result = qry.setColumnFamily(scfName).
      setKey(rowKey).
      setSuperColumn(superColumnName).
      setRange(
        from.map(from => cnSer.toBytes(from)) getOrElse NULL_BYTES,
        to.map(to => cnSer.toBytes(to)) getOrElse NULL_BYTES,
        reversed,
        count).
        execute().
        get().getColumns()

    mapToBeans(rowKey, superColumnName, result)
  }

  private[this] def mapToBeans[SCN, CN, V](rowKey: K, superColumns: Seq[HSuperColumn[SCN, CN, V]]) = {
    for {
      superColumn <- superColumns
      column <- superColumn.getColumns()
    } yield {
      val beanBuilder = bd.newBuilder()
      keyField.toBeanBuilder(rowKey, beanBuilder)
      superColumnNameField.bytesToBeanBuilder(superColumn.getNameByteBuffer(), beanBuilder)
      columnMapper.toBeanBuilder(column, beanBuilder)
      beanBuilder.result().asInstanceOf[B]
    }
  }

  private[this] def mapToBeans[SCN, CN, V](rowKey: K, superColumns: Seq[HSuperColumn[SCN, CN, V]], columnName: CN) = {
    for {
      superColumn <- superColumns
      column <- superColumn.getColumns() if column.getName() == columnName
    } yield {
      val beanBuilder = bd.newBuilder()
      keyField.toBeanBuilder(rowKey, beanBuilder)
      superColumnNameField.bytesToBeanBuilder(superColumn.getNameByteBuffer(), beanBuilder)
      columnMapper.toBeanBuilder(column, beanBuilder)
      beanBuilder.result().asInstanceOf[B]
    }
  }

  private[this] def mapToBeans[CN, V](rowKey: K, superColumnName: SCN, columns: Seq[HColumn[CN, V]]) = {
    for (column <- columns) yield {
      val beanBuilder = bd.newBuilder()
      keyField.toBeanBuilder(rowKey, beanBuilder)
      superColumnNameField.toBeanBuilder(superColumnName, beanBuilder)
      columnMapper.toBeanBuilder(column, beanBuilder)
      beanBuilder.result().asInstanceOf[B]
    }
  }

  private[this] val bd = descriptorOf[B]
  private[this] final val NULL_BYTES = Array.ofDim[Byte](0)
}

class MaterializedViewSubColumnDDD[B <: AnyRef, SCF <: SuperColumnFamily](
  scf: SCF,
  keyField: Mapper[B, SCF#KeyValue],
  keySer: Serializer[SCF#KeyValue],
  superColumnNameField: Mapper[B, SCF#SuperColumnName],
  scnSer: Serializer[SCF#SuperColumnName],
  columnMapper: OCMapper[B, _, _],
  cnSer: Serializer[SCF#ColumnName])(implicit beanMf: Manifest[B]) {

  private[this] val wrappedView = new MaterializedViewSubColumn[B, SCF#KeyValue, SCF#SuperColumnName, SCF#ColumnName](scf.name,
    keyField, keySer, superColumnNameField, scnSer, columnMapper, cnSer)

  def put(bean: B) = wrappedView.put(bean)

  def delete(rowKey: SCF#KeyValue, superColumnName: SCF#SuperColumnName, columnName: SCF#ColumnName) = wrappedView.delete(rowKey, superColumnName, columnName)

  def get(rowKey: SCF#KeyValue, superColumnName: SCF#SuperColumnName, columnName: SCF#ColumnName) = wrappedView.get(rowKey, superColumnName, columnName)

  def getSuperSlice(rowKey: SCF#KeyValue, superColumnName: SCF#SuperColumnName, superColumnNames: SCF#SuperColumnName*) = 
    wrappedView.getSuperSlice(rowKey, superColumnName, superColumnNames: _*)

  def getSuperSlice(
    rowKey: SCF#KeyValue,
    from: Option[SCF#SuperColumnName] = None,
    to: Option[SCF#SuperColumnName] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) = wrappedView.getSuperSlice(rowKey, from, to, count, reversed)

  def getSubSlice(rowKey: SCF#KeyValue, superColumnName: SCF#SuperColumnName, columnName: SCF#ColumnName, columnNames: SCF#ColumnName*) =
    wrappedView.getSubSlice(rowKey, superColumnName, columnName, columnNames: _*)

  def getSubSlice(
    rowKey: SCF#KeyValue,
    superColumnName: SCF#SuperColumnName,
    from: Option[SCF#ColumnName] = None,
    to: Option[SCF#ColumnName] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) = wrappedView.getSubSlice(rowKey, superColumnName, from, to, count, reversed)
}

class MaterializedViewSubColumnSDD[B <: AnyRef, SCF <: SuperColumnFamily](
  scf: SCF,
  keyField: StaticMapper[B, SCF#KeyValue],
  superColumnNameField: Mapper[B, SCF#SuperColumnName],
  scnSer: Serializer[SCF#SuperColumnName],
  columnMapper: OCMapper[B, _, _],
  cnSer: Serializer[SCF#ColumnName])(implicit beanMf: Manifest[B]) {

  private[this] val wrappedView = new MaterializedViewSubColumn[B, Array[Byte], SCF#SuperColumnName, SCF#ColumnName](scf.name,
    keyField.bytesStaticMapper, StandardSerializers.bytesSerializer, superColumnNameField, scnSer, columnMapper, cnSer)

  def put(bean: B) = wrappedView.put(bean)

  def delete(superColumnName: SCF#SuperColumnName, columnName: SCF#ColumnName) = wrappedView.delete(keyField.valueBytes, superColumnName, columnName)

  def get(superColumnName: SCF#SuperColumnName, columnName: SCF#ColumnName) = wrappedView.get(keyField.valueBytes, superColumnName, columnName)

  def getSuperSlice(superColumnName: SCF#SuperColumnName,superColumnNames: SCF#SuperColumnName*) = 
    wrappedView.getSuperSlice(keyField.valueBytes, superColumnName, superColumnNames: _*)

  def getSuperSlice(
    from: Option[SCF#SuperColumnName] = None,
    to: Option[SCF#SuperColumnName] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) = wrappedView.getSuperSlice(keyField.valueBytes, from, to, count, reversed)

  def getSubSlice(superColumnName: SCF#SuperColumnName, columnName: SCF#ColumnName, columnNames: SCF#ColumnName*) =
    wrappedView.getSubSlice(keyField.valueBytes, superColumnName, columnName, columnNames: _*)

  def getSubSlice(
    superColumnName: SCF#SuperColumnName,
    from: Option[SCF#ColumnName] = None,
    to: Option[SCF#ColumnName] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) = wrappedView.getSubSlice(keyField.valueBytes, superColumnName, from, to, count, reversed)
}

class MaterializedViewSubColumnDSD[B <: AnyRef, SCF <: SuperColumnFamily](
  scf: SCF,
  keyField: Mapper[B, SCF#KeyValue],
  keySer: Serializer[SCF#KeyValue],
  superColumnNameField: StaticMapper[B, SCF#SuperColumnName],
  columnMapper: OCMapper[B, _, _],
  cnSer: Serializer[SCF#ColumnName])(implicit beanMf: Manifest[B]) {

  private[this] val wrappedView = new MaterializedViewSubColumn[B, SCF#KeyValue, Array[Byte], SCF#ColumnName](scf.name,
    keyField, keySer, superColumnNameField.bytesStaticMapper, StandardSerializers.bytesSerializer, columnMapper, cnSer)

  def put(bean: B) = wrappedView.put(bean)

  def delete(rowKey: SCF#KeyValue, columnName: SCF#ColumnName) = wrappedView.delete(rowKey, superColumnNameField.valueBytes, columnName)

  def get(rowKey: SCF#KeyValue, columnName: SCF#ColumnName) = wrappedView.get(rowKey, superColumnNameField.valueBytes, columnName)

  def getSubSlice(rowKey: SCF#KeyValue, columnName: SCF#ColumnName, columnNames: SCF#ColumnName*) =
    wrappedView.getSubSlice(rowKey, superColumnNameField.valueBytes, columnName, columnNames: _*)

  def getSubSlice(
    rowKey: SCF#KeyValue,
    from: Option[SCF#ColumnName] = None,
    to: Option[SCF#ColumnName] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) = wrappedView.getSubSlice(rowKey, superColumnNameField.valueBytes, from, to, count, reversed)
}

class MaterializedViewSubColumnSSD[B <: AnyRef, SCF <: SuperColumnFamily](
  scf: SCF,
  keyField: StaticMapper[B, SCF#KeyValue],
  superColumnNameField: StaticMapper[B, SCF#SuperColumnName],
  columnMapper: OCMapper[B, _, _],
  cnSer: Serializer[SCF#ColumnName])(implicit beanMf: Manifest[B]) {

  private[this] val wrappedView = new MaterializedViewSubColumn[B, Array[Byte], Array[Byte], SCF#ColumnName](scf.name,
    keyField.bytesStaticMapper, StandardSerializers.bytesSerializer, superColumnNameField.bytesStaticMapper, StandardSerializers.bytesSerializer, columnMapper, cnSer)

  def put(bean: B) = wrappedView.put(bean)

  def delete(columnName: SCF#ColumnName) = wrappedView.delete(keyField.valueBytes, superColumnNameField.valueBytes, columnName)

  def get(columnName: SCF#ColumnName) = wrappedView.get(keyField.valueBytes, superColumnNameField.valueBytes, columnName)

  def getSubSlice(columnName: SCF#ColumnName, columnNames: SCF#ColumnName*) =
    wrappedView.getSubSlice(keyField.valueBytes, superColumnNameField.valueBytes, columnName, columnNames: _*)

  def getSubSlice(
    from: Option[SCF#ColumnName] = None,
    to: Option[SCF#ColumnName] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) = wrappedView.getSubSlice(keyField.valueBytes, superColumnNameField.valueBytes, from, to, count, reversed)
}

class MaterializedViewSubColumnDDS[B <: AnyRef, SCF <: SuperColumnFamily](
  scf: SCF,
  keyField: Mapper[B, SCF#KeyValue],
  keySer: Serializer[SCF#KeyValue],
  superColumnNameField: Mapper[B, SCF#SuperColumnName],
  scnSer: Serializer[SCF#SuperColumnName],
  columnMapper: StaticColumnMapper[B, _, _])(implicit beanMf: Manifest[B]) {

  private[this] val wrappedView = new MaterializedViewSubColumn[B, SCF#KeyValue, SCF#SuperColumnName, Array[Byte]](scf.name,
    keyField, keySer, superColumnNameField, scnSer, columnMapper, StandardSerializers.bytesSerializer)

  def put(bean: B) = wrappedView.put(bean)

  def delete(rowKey: SCF#KeyValue, superColumnName: SCF#SuperColumnName) = wrappedView.delete(rowKey, superColumnName, columnMapper.columnNameBytes)

  def get(rowKey: SCF#KeyValue, superColumnName: SCF#SuperColumnName) = wrappedView.get(rowKey, superColumnName, columnMapper.columnNameBytes)

  def getSuperSlice(rowKey: SCF#KeyValue, superColumnName: SCF#SuperColumnName, superColumnNames: SCF#SuperColumnName*) =
    wrappedView.getSuperSliceSubColumn(rowKey, columnMapper.columnNameBytes, superColumnName, superColumnNames: _*)

  def getSuperSlice(
    rowKey: SCF#KeyValue,
    from: Option[SCF#SuperColumnName] = None,
    to: Option[SCF#SuperColumnName] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) =
    wrappedView.getSuperSliceSubColumn(rowKey, columnMapper.columnNameBytes, from, to, count, reversed)
}

class MaterializedViewSubColumnSDS[B <: AnyRef, SCF <: SuperColumnFamily](
  scf: SCF,
  keyField: StaticMapper[B, SCF#KeyValue],
  superColumnNameField: Mapper[B, SCF#SuperColumnName],
  scnSer: Serializer[SCF#SuperColumnName],
  columnMapper: StaticColumnMapper[B, _, _])(implicit beanMf: Manifest[B]) {

  private[this] val wrappedView = new MaterializedViewSubColumn[B, Array[Byte], SCF#SuperColumnName, Array[Byte]](scf.name,
    keyField.bytesStaticMapper, StandardSerializers.bytesSerializer, superColumnNameField, scnSer, columnMapper, StandardSerializers.bytesSerializer)

  def put(bean: B) = wrappedView.put(bean)

  def delete(superColumnName: SCF#SuperColumnName) = wrappedView.delete(keyField.valueBytes, superColumnName, columnMapper.columnNameBytes)

  def get(superColumnName: SCF#SuperColumnName) = wrappedView.get(keyField.valueBytes, superColumnName, columnMapper.columnNameBytes)

  def getSuperSlice(superColumnName: SCF#SuperColumnName, superColumnNames: SCF#SuperColumnName*) = 
    wrappedView.getSuperSliceSubColumn(keyField.valueBytes, columnMapper.columnNameBytes, superColumnName, superColumnNames: _*)

  def getSuperSlice(
    from: Option[SCF#SuperColumnName] = None,
    to: Option[SCF#SuperColumnName] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) = wrappedView.getSuperSliceSubColumn(keyField.valueBytes, columnMapper.columnNameBytes, from, to, count, reversed)
}

class MaterializedViewSubColumnDSS[B <: AnyRef, SCF <: SuperColumnFamily](
  scf: SCF,
  keyField: Mapper[B, SCF#KeyValue],
  keySer: Serializer[SCF#KeyValue],
  superColumnNameField: StaticMapper[B, SCF#SuperColumnName],
  columnMapper: StaticColumnMapper[B, _, _])(implicit beanMf: Manifest[B]) {

  private[this] val wrappedView = new MaterializedViewSubColumn[B, SCF#KeyValue, Array[Byte], Array[Byte]](scf.name,
    keyField, keySer, superColumnNameField.bytesStaticMapper, StandardSerializers.bytesSerializer, columnMapper, StandardSerializers.bytesSerializer)

  def put(bean: B) = wrappedView.put(bean)

  def delete(rowKey: SCF#KeyValue) = wrappedView.delete(rowKey, superColumnNameField.valueBytes, columnMapper.columnNameBytes)

  def get(rowKey: SCF#KeyValue) = wrappedView.get(rowKey, superColumnNameField.valueBytes, columnMapper.columnNameBytes)
}

class MaterializedViewSubColumnSSS[B <: AnyRef, SCF <: SuperColumnFamily](
  scf: SCF,
  keyField: StaticMapper[B, SCF#KeyValue],
  superColumnNameField: StaticMapper[B, SCF#SuperColumnName],
  columnMapper: StaticColumnMapper[B, _, _])(implicit beanMf: Manifest[B]) {

  private[this] val wrappedView = new MaterializedViewSubColumn[B, Array[Byte], Array[Byte], Array[Byte]](scf.name,
    keyField.bytesStaticMapper, StandardSerializers.bytesSerializer, superColumnNameField.bytesStaticMapper, StandardSerializers.bytesSerializer,
    columnMapper, StandardSerializers.bytesSerializer)

  def put(bean: B) = wrappedView.put(bean)

  def delete() = wrappedView.delete(keyField.valueBytes, superColumnNameField.valueBytes, columnMapper.columnNameBytes)

  def get() = wrappedView.get(keyField.valueBytes, superColumnNameField.valueBytes, columnMapper.columnNameBytes)
}