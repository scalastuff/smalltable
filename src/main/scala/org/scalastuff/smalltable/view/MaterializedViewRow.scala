package org.scalastuff.smalltable.view

import scala.collection.JavaConversions._

import org.scalastuff.scalabeans.Preamble.descriptorOf
import org.scalastuff.smalltable.mapper._
import org.scalastuff.smalltable.serde.StandardSerializers
import org.scalastuff.smalltable.ColumnFamily

import me.prettyprint.cassandra.model.CqlQuery
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.mutation.Mutator
import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.Serializer

/**
 * Maps row to a bean
 */
private[view] class MaterializedViewRow[B <: AnyRef, K](
  cfName: String,
  keyField: Mapper[B, K],
  columnsMapper: OCsMapper[B],
  keySerializer: Serializer[K])(implicit beanMf: Manifest[B]) {

  def put(bean: B) = UpdateQuery(keySerializer, { mutator: Mutator[K] =>
      val rowKey = keyField.toValue(bean)
      for (column <- columnsMapper.toColumns(bean))
        mutator.addInsertion(rowKey, cfName, column)
  })
  
  def putAll(beans: Traversable[B]) = UpdateQuery(keySerializer, { mutator: Mutator[K] =>
    for (bean <- beans) {
      val rowKey = keyField.toValue(bean)
      for (column <- columnsMapper.toColumns(bean))
        mutator.addInsertion(rowKey, cfName, column)
    }
  })

  def delete(rowKeys: K*) = deleteAll(rowKeys)

  def deleteAll(rowKeys: Traversable[K]) = UpdateQuery(keySerializer, { mutator: Mutator[K] =>
    for (rowKey <- rowKeys)
      mutator.addDeletion(rowKey, cfName)
  })
  
  def get(rowKey: K) = { keyspace: Keyspace =>
    val sliceQuery = HFactory.createSliceQuery(keyspace, keySerializer, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer)
    val result = sliceQuery.
      setColumnFamily(cfName).
      setKey(rowKey).
      setColumnNames(columnsMapper.columnNames: _*).
      execute().
      get().getColumns
    if (result.isEmpty) None
    else {
      val beanBuilder = bd.newBuilder()
      keyField.toBeanBuilder(rowKey, beanBuilder)
      columnsMapper.toBeanBuilder(result, beanBuilder)
      Some(beanBuilder.result().asInstanceOf[B])
    }
  }

  def select(where: String) = { keyspace: Keyspace =>
    val cql = new CqlQuery[K, Array[Byte], Array[Byte]](keyspace, keySerializer, StandardSerializers.bytesSerializer, StandardSerializers.bytesSerializer)
    val cqlQuery = this.cqlSelect + (if (!where.isEmpty) " where " + where else "")
    cql.setQuery(cqlQuery)
    val cqlResult = cql.execute().get().iterator().toStream
    for (row <- cqlResult if !row.getColumnSlice().getColumns().isEmpty) yield {
      val beanBuilder = bd.newBuilder()
      keyField.toBeanBuilder(row.getKey(), beanBuilder)
      columnsMapper.toBeanBuilder(row.getColumnSlice().getColumns(), beanBuilder)
      beanBuilder.result().asInstanceOf[B]
    }
  }
  
  private[this] val bd = descriptorOf[B]
  private[this] val cqlSelect = "select KEY," + (columnsMapper.columnMappers.map(_.columnName) mkString ",") + " from " + cfName
}

/**
 * Maps row to a bean. Row key is mapped to a bean property
 */
class MaterializedViewRowDK[B <: AnyRef, CF <: ColumnFamily](
  cf: CF,
  keyField: Mapper[B, CF#KeyValue],
  columnsMapper: OCsMapper[B],
  keySerializer: Serializer[CF#KeyValue])(implicit beanMf: Manifest[B]) {
  
  private[this] val wrappedView = new MaterializedViewRow[B, CF#KeyValue](cf.name, keyField, columnsMapper, keySerializer)
  
  def put(beans: B*) = wrappedView.putAll(beans)
  def putAll(beans: Traversable[B]) = wrappedView.putAll(beans)
  def delete(rowKey: CF#KeyValue) = wrappedView.delete(rowKey)
  def deleteAll(rowKeys: Traversable[CF#KeyValue]) = wrappedView.deleteAll(rowKeys)
  def get(rowKey: CF#KeyValue) = wrappedView.get(rowKey)
  def select(where: String) = wrappedView.select(where)
}

/**
 * Maps row to a bean. Row key is a constant
 */
class MaterializedViewRowSK[B <: AnyRef, CF <: ColumnFamily](
  cf: CF,
  keyField: StaticMapper[B, CF#KeyValue],
  columnsMapper: OCsMapper[B])(implicit beanMf: Manifest[B]) {
  
  private[this] val wrappedView = new MaterializedViewRow[B, Array[Byte]](cf.name, keyField.bytesStaticMapper, columnsMapper, StandardSerializers.bytesSerializer)
  
  def put(bean: B) = wrappedView.put(bean)
  def delete() = wrappedView.delete(keyField.valueBytes)
  def get() = wrappedView.get(keyField.valueBytes)
}