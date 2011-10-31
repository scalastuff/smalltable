package org.scalastuff.smalltable.view

import scala.annotation.implicitNotFound
import scala.collection.JavaConversions._

import org.scalastuff.smalltable.mapper._
import org.scalastuff.smalltable.SuperColumnFamily
import org.scalastuff.smalltable.serde.StandardSerializers
import org.scalastuff.scalabeans.Preamble.descriptorOf

import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.mutation.Mutator
import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.Serializer

/**
 * Maps a bean to a SuperColumn. View contains following fields: 
 * - row key
 * - SuperColumn name
 * - Columns are mapped to fields: column(name, value) <=> field(name, value)
 * 
 * Each row can contain many beans, each SuperColumn is mapped to 1 bean.
 * Unique key is (rowKey, superColumnName).
 * Search can be done on rowKey (exact match) and superColumnName (exact match or range). 
 * rowKey is always required for search.
 * 
 * Subclass SuperColumnFamily and create this view as a member value. Instances are thread-safe
 * and can be reused.
 *  
 * Example:
 * {{{
 * object MyBeanSuperColumnFamily extends SuperColumnFamily("MyBean") {
 *   ...
 *   val myBeanView = materializedView[MyBean].rowKeyProperty("groupBy").columnNameProperty("id").staticColumnNames()
 * }
 * }}}
 */
private[view] class MaterializedViewSuperColumn[B <: AnyRef, K, SCN](
  scfName: String,
  keyField: Mapper[B, K],
  keySer: Serializer[K],
  superColumnNameField: Mapper[B, SCN],
  scnSer: Serializer[SCN],
  columnsMapper: OCsMapper[B])(implicit beanMf: Manifest[B]) {

  def put(bean: B) = UpdateQuery(keySer, { mutator: Mutator[K] =>
    val rowKey = keyField.toValue(bean)
    val superColumName = superColumnNameField.toValue(bean)
    val superColumn = HFactory.createSuperColumn(
      superColumName,
      columnsMapper.toColumns(bean).asInstanceOf[Iterable[HColumn[Array[Byte], Array[Byte]]]].toSeq,
      scnSer, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer)
    mutator.addInsertion(rowKey, scfName, superColumn)
  })

  def delete(rowKey: K, superColumnName: SCN) = UpdateQuery(keySer, { mutator: Mutator[K] =>
    mutator.addSuperDelete(rowKey, scfName, superColumnName, scnSer)
  })

  def get(rowKey: K, superColumnName: SCN) = { keyspace: Keyspace =>
    val qry = HFactory.createSubSliceQuery(keyspace, keySer, scnSer, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer)
    val result = qry.setColumnFamily(scfName).
      setKey(rowKey).
      setSuperColumn(superColumnName).
      setColumnNames(columnsMapper.columnNames: _*).
      execute().
      get().getColumns()

    if (result.isEmpty) None
    else {
      val beanBuilder = bd.newBuilder()
      keyField.toBeanBuilder(rowKey, beanBuilder)
      superColumnNameField.toBeanBuilder(superColumnName, beanBuilder)
      columnsMapper.toBeanBuilder(result, beanBuilder)
      Some(beanBuilder.result().asInstanceOf[B])
    }
  }

  def getSlice(rowKey: K, superColumnName: SCN, superColumnNames: SCN*) = { keyspace: Keyspace =>
    val qry = HFactory.createSuperSliceQuery(keyspace, keySer, scnSer, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer)
    val result = qry.setColumnFamily(scfName).
      setKey(rowKey).
      setColumnNames((superColumnName +: superColumnNames): _*).
      execute().
      get().getSuperColumns()

    for (superColumn <- result) yield {
      val beanBuilder = bd.newBuilder()
      keyField.toBeanBuilder(rowKey, beanBuilder)
      superColumnNameField.bytesToBeanBuilder(superColumn.getNameByteBuffer(), beanBuilder)
      columnsMapper.toBeanBuilder(superColumn.getColumns(), beanBuilder)
      beanBuilder.result().asInstanceOf[B]
    }
  }

  def getSlice(
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

    for (superColumn <- result) yield {
      val beanBuilder = bd.newBuilder()
      keyField.toBeanBuilder(rowKey, beanBuilder)
      superColumnNameField.bytesToBeanBuilder(superColumn.getNameByteBuffer(), beanBuilder)
      columnsMapper.toBeanBuilder(superColumn.getColumns(), beanBuilder)
      beanBuilder.result().asInstanceOf[B]
    }
  }

  private[this] val bd = descriptorOf[B]
  private[this] final val NULL_BYTES = Array.ofDim[Byte](0)
}

class MaterializedViewSuperColumnDKDC[B <: AnyRef, SCF <: SuperColumnFamily](
  scf: SCF,
  keyField: Mapper[B, SCF#KeyValue],
  keySer: Serializer[SCF#KeyValue],
  superColumnNameField: Mapper[B, SCF#SuperColumnName],
  scnSer: Serializer[SCF#SuperColumnName],
  columnsMapper: OCsMapper[B])(implicit beanMf: Manifest[B]) {
  
  private[this] val wrappedView = new MaterializedViewSuperColumn[B, SCF#KeyValue, SCF#SuperColumnName](scf.name, 
      keyField, keySer, superColumnNameField, scnSer, columnsMapper) 

  def put(bean: B) = wrappedView.put(bean)

  def delete(rowKey: SCF#KeyValue, superColumnName: SCF#SuperColumnName) = wrappedView.delete(rowKey, superColumnName)

  def get(rowKey: SCF#KeyValue, superColumnName: SCF#SuperColumnName) = wrappedView.get(rowKey, superColumnName)

  def getSlice(rowKey: SCF#KeyValue, superColumnName: SCF#SuperColumnName, superColumnNames: SCF#SuperColumnName*) = wrappedView.getSlice(rowKey, superColumnName, superColumnNames: _*)

  def getSlice(
    rowKey: SCF#KeyValue,
    from: Option[SCF#SuperColumnName] = None,
    to: Option[SCF#SuperColumnName] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) = wrappedView.getSlice(rowKey, from, to, count, reversed)
}

class MaterializedViewSuperColumnSKDC[B <: AnyRef, SCF <: SuperColumnFamily](
  scf: SCF,
  keyField: StaticMapper[B, SCF#KeyValue],
  superColumnNameField: Mapper[B, SCF#SuperColumnName],
  scnSer: Serializer[SCF#SuperColumnName],
  columnsMapper: OCsMapper[B])(implicit beanMf: Manifest[B]) {

  private[this] val wrappedView = new MaterializedViewSuperColumn[B, Array[Byte], SCF#SuperColumnName](scf.name, 
      keyField.bytesStaticMapper, StandardSerializers.bytesSerializer, superColumnNameField, scnSer, columnsMapper)
  
  def put(bean: B) = wrappedView.put(bean)

  def delete(superColumnName: SCF#SuperColumnName) = wrappedView.delete(keyField.valueBytes, superColumnName)

  def get(superColumnName: SCF#SuperColumnName) = wrappedView.get(keyField.valueBytes, superColumnName)

  def getSlice(superColumnName: SCF#SuperColumnName, superColumnNames: SCF#SuperColumnName*) = wrappedView.getSlice(keyField.valueBytes, superColumnName, superColumnNames: _*)

  def getSlice(
    from: Option[SCF#SuperColumnName] = None,
    to: Option[SCF#SuperColumnName] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) = wrappedView.getSlice(keyField.valueBytes, from, to, count, reversed)
}

class MaterializedViewSuperColumnDKSC[B <: AnyRef, SCF <: SuperColumnFamily](
  scf: SCF,
  keyField: Mapper[B, SCF#KeyValue],
  keySer: Serializer[SCF#KeyValue],
  superColumnNameField: StaticMapper[B, SCF#SuperColumnName],
  columnsMapper: OCsMapper[B])(implicit beanMf: Manifest[B]) {
  
  private[this] val wrappedView = new MaterializedViewSuperColumn[B, SCF#KeyValue, Array[Byte]](scf.name, 
      keyField, keySer, superColumnNameField.bytesStaticMapper, StandardSerializers.bytesSerializer, columnsMapper) 

  def put(bean: B) = wrappedView.put(bean)

  def delete(rowKey: SCF#KeyValue) = wrappedView.delete(rowKey, superColumnNameField.valueBytes)

  def get(rowKey: SCF#KeyValue) = wrappedView.get(rowKey, superColumnNameField.valueBytes)
}

class MaterializedViewSuperColumnSKSC[B <: AnyRef, SCF <: SuperColumnFamily](
  scf: SCF,
  keyField: StaticMapper[B, SCF#KeyValue],
  superColumnNameField: StaticMapper[B, SCF#SuperColumnName],
  columnsMapper: OCsMapper[B])(implicit beanMf: Manifest[B]) {
  
  private[this] val wrappedView = new MaterializedViewSuperColumn[B, Array[Byte], Array[Byte]](scf.name, 
      keyField.bytesStaticMapper, StandardSerializers.bytesSerializer, superColumnNameField.bytesStaticMapper, StandardSerializers.bytesSerializer, columnsMapper) 

  def put(bean: B) = wrappedView.put(bean)

  def delete() = wrappedView.delete(keyField.valueBytes, superColumnNameField.valueBytes)

  def get() = wrappedView.get(keyField.valueBytes, superColumnNameField.valueBytes)
}