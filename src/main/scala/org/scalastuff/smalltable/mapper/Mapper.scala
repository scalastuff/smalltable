package org.scalastuff.smalltable.mapper

import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import org.scalastuff.scalabeans.Preamble._
import org.scalastuff.scalabeans.BeanBuilder
import org.scalastuff.scalabeans.PropertyDescriptor
import org.scalastuff.scalabeans.ManifestFactory
import com.dyuproject.protostuff.ByteArrayInput
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.Serializer
import me.prettyprint.cassandra.model.BasicColumnDefinition
import me.prettyprint.hector.api.ddl.ColumnIndexType
import me.prettyprint.cassandra.service.ThriftColumnDef
import org.scalastuff.smalltable.serde.SerializersRegistry
import org.scalastuff.smalltable.serde.StandardSerializers

trait Mapper[B <: AnyRef, V] {
  def valueSerializer: Serializer[V]
  def toValue(bean: B): V
  def toValueBytes(bean: B) = valueSerializer.toBytes(toValue(bean))
  def toBeanBuilder(value: V, beanBuilder: BeanBuilder): Unit
  def bytesToBeanBuilder(buffer: ByteBuffer, beanBuilder: BeanBuilder): Unit = {
    val value = valueSerializer.fromByteBuffer(buffer)
    toBeanBuilder(value, beanBuilder)
  }
  def propertyNames: Iterable[String]
}

/**
 * Maps bean to static (predefined) value
 */
class StaticMapper[B <: AnyRef, V](value: V, val valueSerializer: Serializer[V]) extends Mapper[B, V] { self =>
  def toValue(bean: B) = value
  val valueBytes = valueSerializer.toBytes(value)
  override def toValueBytes(bean: B) = valueBytes
  def toBeanBuilder(value: V, beanBuilder: BeanBuilder): Unit = {}
  def bytesToBeanBuilder(value: Array[Byte], beanBuilder: BeanBuilder): Unit = {}
  final val propertyNames = Nil

  /**
   * Convert StaticMapper[B, V] to StaticMapper[B, Array[Byte]] using same value
   */
  lazy val bytesStaticMapper = new Mapper[B, Array[Byte]] {
    def valueSerializer = StandardSerializers.bytesSerializer
    def toValue(bean: B) = self.valueBytes
    override def toValueBytes(bean: B) = self.valueBytes
    def toBeanBuilder(value: Array[Byte], beanBuilder: BeanBuilder): Unit = {}
    def bytesToBeanBuilder(value: Array[Byte], beanBuilder: BeanBuilder): Unit = {}
    final val propertyNames = Nil
  }
}

class PropertyMapper[B <: AnyRef, V] private (propertyDescriptor: PropertyDescriptor, val valueSerializer: Serializer[V]) extends Mapper[B, V] {
  def toValue(bean: B) = propertyDescriptor.get[V](bean)
  def toBeanBuilder(value: V, beanBuilder: BeanBuilder) = {
    beanBuilder.set(propertyDescriptor, value)
  }
  val propertyNames = List(propertyDescriptor.name)
}
object PropertyMapper {
  def apply[B <: AnyRef](propertyDescriptor: PropertyDescriptor, registry: SerializersRegistry) = {
    new PropertyMapper[B, Any](propertyDescriptor, registry.forProperty[Any](propertyDescriptor.name, propertyDescriptor.scalaType))
  }

  def apply[B <: AnyRef, V](propertyDescriptor: PropertyDescriptor, valueSerializer: Serializer[V])(implicit valueMf: Manifest[V]) = {
    if (!(valueMf >:> ManifestFactory.manifestOf(propertyDescriptor.scalaType)))
      throw new RuntimeException("Cannot create property mapper for %s %s with type %s: property is of %s type".
        format(propertyDescriptor.beanType, propertyDescriptor.name, valueMf, propertyDescriptor.scalaType))

    new PropertyMapper[B, V](propertyDescriptor, valueSerializer)
  }
}

class ObjectMapper[B <: AnyRef](excludeProperties: Iterable[String])(implicit beanMf: Manifest[B]) extends Mapper[B, Array[Byte]] {
  import org.scalastuff.proto.Preamble._
  import org.scalastuff.proto._

  val bd = descriptorOf[B]
  val writer = writerOf[B]
  val format = ProtobufFormat

  val valueSerializer = StandardSerializers.bytesSerializer

  def toValue(bean: B) = writer.toByteArray(bean, format) //TODO: excludeProperties
  def toBeanBuilder(value: Array[Byte], beanBuilder: BeanBuilder) = {
    val beanBuilderSchema = BeanBuilderSchema(bd)
    val input = new ByteArrayInput(value, false)
    beanBuilderSchema.mergeFrom(input, beanBuilder)
  }
  val propertyNames = bd.properties.map(_.name).filter(name => !excludeProperties.contains(name))
}

/**
 * Object to Columns mapper
 */
class OCsMapper[B <: AnyRef](
  registry: SerializersRegistry,
  excludeProperties: Iterable[String],
  rename: Iterable[(String, String)],
  index: Iterable[String] = Nil)(implicit beanMf: Manifest[B]) {
  private[this] val bd = descriptorOf[B]
  val columnMappers =
    for (p <- bd.properties if !excludeProperties.contains(p.name)) yield {
      val propertyMapper = PropertyMapper[B](p, registry)
      rename.find(propName2colName => propName2colName._1 == p.name) match {
        case Some((_, columnName)) => new StaticColumnMapper[B, String, Any](columnName, StandardSerializers.stringSerializer, propertyMapper)
        case None                  => new StaticColumnMapper[B, String, Any](p.name, StandardSerializers.stringSerializer, propertyMapper)
      }
    }
  val columnNames = columnMappers.map(_.columnNameBytes).toArray

  def toColumns(bean: B): Iterable[HColumn[_, _]] = {
    val clock = HFactory.createClock()
    for (mapper <- columnMappers) yield mapper.toColumn(bean, clock)
  }
  def toBeanBuilder(columns: Iterable[HColumn[Array[Byte], _]], beanBuilder: BeanBuilder) {
    for (mapper <- columnMappers) {
      columns.find(column => java.util.Arrays.equals(column.getName(), mapper.columnNameBytes)) match {
        case Some(column) => mapper.toBeanBuilder(column, beanBuilder)
        case None         => // ignore, property not found in the result
      }
    }
  }

  def columnDefinitions() = {
    for (columnMapper <- columnMappers) yield {
      val columnName = columnMapper.columnName
      val columnDef = new BasicColumnDefinition()
      columnDef.setName(StandardSerializers.stringSerializer.toByteBuffer(columnName))
      columnDef.setValidationClass(columnMapper.valueSerializer.getComparatorType.getClassName)
      if (index.contains(columnName)) {
        columnDef.setIndexType(ColumnIndexType.KEYS)
        columnDef.setIndexName("idx_" + bd.beanType + "_" + columnName)
      }

      columnDef
    }
  }
}

trait OCMapper[B <: AnyRef, N, V] {
  def toColumn(bean: B): HColumn[N, V]
  def toBeanBuilder(column: HColumn[_, _], beanBuilder: BeanBuilder): Unit
}

/**
 * Column name and value are mapped to bean properties
 */
class DynamicColumnMapper[B <: AnyRef, N, V](nameMapper: Mapper[B, N], valueMapper: Mapper[B, V])
  extends OCMapper[B, N, V] {
  def toColumn(bean: B): HColumn[N, V] = {
    HFactory.createColumn(nameMapper.toValue(bean), valueMapper.toValue(bean), nameMapper.valueSerializer, valueMapper.valueSerializer)
  }
  def toBeanBuilder(column: HColumn[_, _], beanBuilder: BeanBuilder) {
    nameMapper.bytesToBeanBuilder(column.getNameBytes(), beanBuilder)
    valueMapper.bytesToBeanBuilder(column.getValueBytes(), beanBuilder)
  }
}

/**
 * Constant column name, value is mapped to a bean property
 */
class StaticColumnMapper[B <: AnyRef, N, V](val columnName: N, columnNameSerializer: Serializer[N], valueMapper: Mapper[B, V])
  extends OCMapper[B, Array[Byte], V] {
  def valueSerializer = valueMapper.valueSerializer
  val columnNameBytes: Array[Byte] = columnNameSerializer.toBytes(columnName)

  def toColumn(bean: B, clock: Long): HColumn[Array[Byte], V] = {
    HFactory.createColumn(columnNameBytes, valueMapper.toValue(bean), clock, StandardSerializers.bytesSerializer, valueSerializer)
  }

  def toColumn(bean: B): HColumn[Array[Byte], V] = {
    HFactory.createColumn(columnNameBytes, valueMapper.toValue(bean), StandardSerializers.bytesSerializer, valueSerializer)
  }

  def toBeanBuilder(column: HColumn[_, _], beanBuilder: BeanBuilder) {
    valueMapper.bytesToBeanBuilder(column.getValueBytes(), beanBuilder)
  }
}