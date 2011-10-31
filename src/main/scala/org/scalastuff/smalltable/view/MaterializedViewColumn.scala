package org.scalastuff.smalltable.view

import scala.collection.JavaConversions._
import org.scalastuff.smalltable.mapper._
import org.scalastuff.smalltable.ColumnFamily
import org.scalastuff.scalabeans.Preamble._
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.mutation.Mutator
import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.Serializer
import org.scalastuff.smalltable.serde.StandardSerializers

/**
 * Maps Column to a bean.
 */
private[view] class MaterializedViewColumn[B <: AnyRef, K, CN](
  cfName: String,
  keyField: Mapper[B, K],
  columnMapper: OCMapper[B, _, _],
  keySerializer: Serializer[K],
  columnNameSerializer: Serializer[CN])(implicit beanMf: Manifest[B]) {

  def put(bean: B) = UpdateQuery(keySerializer, { mutator: Mutator[K] =>
    val rowKey = keyField.toValue(bean)
    val column = columnMapper.toColumn(bean)
    mutator.addInsertion(rowKey, cfName, column)
  })

  def putAll(beans: Traversable[B]) = UpdateQuery(keySerializer, { mutator: Mutator[K] =>
    for (bean <- beans) {
      val rowKey = keyField.toValue(bean)
      val column = columnMapper.toColumn(bean)
      mutator.addInsertion(rowKey, cfName, column)
    }
  })

  def delete(rowKey: K, columnName: CN) = UpdateQuery(keySerializer, { mutator: Mutator[K] =>
    mutator.addDeletion(rowKey, cfName, columnName, columnNameSerializer)
  })

  def get(rowKey: K, columnName: CN) = { keyspace: Keyspace =>
    val sliceQuery = HFactory.createColumnQuery(keyspace, keySerializer, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer)
    val result = sliceQuery.
      setColumnFamily(cfName).
      setKey(rowKey).
      setName(columnNameSerializer.toBytes(columnName)).
      execute().
      get()
    if (result eq null) None
    else {
      val beanBuilder = bd.newBuilder()
      keyField.toBeanBuilder(rowKey, beanBuilder)
      columnMapper.toBeanBuilder(result, beanBuilder)
      Some(beanBuilder.result().asInstanceOf[B])
    }
  }

  def getSlice(rowKey: K, columnName: CN, columnNames: Seq[CN]) = { keyspace: Keyspace =>
    val sliceQuery = HFactory.createSliceQuery(keyspace, keySerializer, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer)
    val result = sliceQuery.
      setColumnFamily(cfName).
      setKey(rowKey).
      setColumnNames((columnName +: columnNames).map(cn => columnNameSerializer.toBytes(cn)): _*).
      execute().
      get().getColumns()
    for (column <- result) yield {
      val beanBuilder = bd.newBuilder()
      keyField.toBeanBuilder(rowKey, beanBuilder)
      columnMapper.toBeanBuilder(column, beanBuilder)
      beanBuilder.result().asInstanceOf[B]
    }
  }

  def getSlice(rowKey: K, from: Option[CN] = None, to: Option[CN] = None, count: Int = Int.MaxValue, reversed: Boolean = false) = { keyspace: Keyspace =>
    val sliceQuery = HFactory.createSliceQuery(keyspace, keySerializer, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer)
    val result = sliceQuery.
      setColumnFamily(cfName).
      setKey(rowKey).
      setRange(
        from.map(from => columnNameSerializer.toBytes(from)) getOrElse NULL_BYTES,
        to.map(to => columnNameSerializer.toBytes(to)) getOrElse NULL_BYTES,
        reversed,
        count).
        execute().
        get().getColumns()
    for (column <- result) yield {
      val beanBuilder = bd.newBuilder()
      keyField.toBeanBuilder(rowKey, beanBuilder)
      columnMapper.toBeanBuilder(column, beanBuilder)
      beanBuilder.result().asInstanceOf[B]
    }
  }

  private[this] final val NULL_BYTES = Array.ofDim[Byte](0)
  private[this] val bd = descriptorOf[B]
}

/**
 * Maps Column to a bean.
 *
 * Row key and column name are mapped to bean properties
 */
class MaterializedViewColumnDKDC[B <: AnyRef, CF <: ColumnFamily](
  cf: CF,
  keyField: Mapper[B, CF#KeyValue],
  columnMapper: OCMapper[B, _, _],
  keySerializer: Serializer[CF#KeyValue],
  columnNameSerializer: Serializer[CF#ColumnName])(implicit beanMf: Manifest[B]) {

  private[this] val wrappedView = new MaterializedViewColumn[B, CF#KeyValue, CF#ColumnName](cf.name, keyField, columnMapper, keySerializer, columnNameSerializer)

  def put(beans: B*) = wrappedView.putAll(beans)
  def delete(rowKey: CF#KeyValue, columnName: CF#ColumnName) = wrappedView.delete(rowKey, columnName)

  def get(rowKey: CF#KeyValue, columnName: CF#ColumnName) = wrappedView.get(rowKey, columnName)

  def getSlice(rowKey: CF#KeyValue, columnName: CF#ColumnName, columnNames: CF#ColumnName*) = wrappedView.getSlice(rowKey, columnName, columnNames)

  def getSlice(rowKey: CF#KeyValue,
               from: Option[CF#ColumnName] = None,
               to: Option[CF#ColumnName] = None,
               count: Int = Int.MaxValue,
               reversed: Boolean = false) = wrappedView.getSlice(rowKey, from, to, count, reversed)
}

/**
 * Maps Column to a bean.
 *
 * Row key is constant, column name is mapped to a bean property
 */
class MaterializedViewColumnSKDC[B <: AnyRef, CF <: ColumnFamily](
  cf: CF,
  keyField: StaticMapper[B, CF#KeyValue],
  columnMapper: OCMapper[B, _, _],
  columnNameSerializer: Serializer[CF#ColumnName])(implicit beanMf: Manifest[B]) {

  private[this] val wrappedView = new MaterializedViewColumn[B, Array[Byte], CF#ColumnName](cf.name,
    keyField.bytesStaticMapper, columnMapper, StandardSerializers.bytesSerializer, columnNameSerializer)

  def put(beans: B*) = wrappedView.putAll(beans)
  def delete(columnName: CF#ColumnName) = wrappedView.delete(keyField.valueBytes, columnName)

  def get(columnName: CF#ColumnName) = wrappedView.get(keyField.valueBytes, columnName)

  def getSlice(columnName: CF#ColumnName, columnNames: CF#ColumnName*) = wrappedView.getSlice(keyField.valueBytes, columnName, columnNames)

  def getSlice(
    from: Option[CF#ColumnName] = None,
    to: Option[CF#ColumnName] = None,
    count: Int = Int.MaxValue,
    reversed: Boolean = false) = wrappedView.getSlice(keyField.valueBytes, from, to, count, reversed)
}

/**
 * Maps Column to a bean.
 *
 * Row key is mapped to a bean property, column name is a constant
 */
class MaterializedViewColumnDKSC[B <: AnyRef, CF <: ColumnFamily](
  cf: CF,
  keyField: Mapper[B, CF#KeyValue],
  columnMapper: StaticColumnMapper[B, _, _],
  keySerializer: Serializer[CF#KeyValue])(implicit beanMf: Manifest[B]) {

  private[this] val wrappedView = new MaterializedViewColumn[B, CF#KeyValue, Array[Byte]](cf.name, keyField, columnMapper, keySerializer, StandardSerializers.bytesSerializer)

  def put(beans: B*) = wrappedView.putAll(beans)
  def delete(rowKey: CF#KeyValue) = wrappedView.delete(rowKey, columnMapper.columnNameBytes)

  def get(rowKey: CF#KeyValue) = wrappedView.get(rowKey, columnMapper.columnNameBytes)
}

/**
 * Maps Column to a bean.
 *
 * Row key and column name are constant
 */
class MaterializedViewColumnSKSC[B <: AnyRef, CF <: ColumnFamily](
  cf: CF,
  keyField: StaticMapper[B, CF#KeyValue],
  columnMapper: StaticColumnMapper[B, _, _])(implicit beanMf: Manifest[B]) {

  private[this] val wrappedView = new MaterializedViewColumn[B, Array[Byte], Array[Byte]](cf.name,
    keyField.bytesStaticMapper, columnMapper, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer)

  def put(bean: B) = wrappedView.put(bean)
  def delete(columnName: CF#ColumnName) = wrappedView.delete(keyField.valueBytes, columnMapper.columnNameBytes)

  def get(columnName: CF#ColumnName) = wrappedView.get(keyField.valueBytes, columnMapper.columnNameBytes)
}
