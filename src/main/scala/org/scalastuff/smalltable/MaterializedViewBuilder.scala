package org.scalastuff.smalltable

import org.scalastuff.scalabeans.Preamble._
import org.scalastuff.smalltable.mapper._
import me.prettyprint.hector.api.Serializer
import org.scalastuff.smalltable.serde.SerializersRegistry
import org.scalastuff.smalltable.serde.StandardSerializers


protected[smalltable] class MaterializedViewBuilderCV[B <: AnyRef: Manifest, CN: Manifest, DCNV, SCNV](
  excludeProperties: Iterable[String],
  serializers: SerializersRegistry,
  columnNameSerializer: Serializer[CN],
  createDynamicColumnNameView: OCMapper[B, _, _] => DCNV,
  createStaticColumnNameView: StaticColumnMapper[B, _, _] => SCNV) {
  
  def columnNameProperty(subColumnNamePropertyName: String) = new {
    def valueProperty(valuePropertyName: String) = {
      val bd = descriptorOf[B]

      val columnMapper =
        new DynamicColumnMapper[B, CN, Any](
          PropertyMapper[B, CN](bd(subColumnNamePropertyName), columnNameSerializer),
          PropertyMapper[B](bd(valuePropertyName), serializers))

      createDynamicColumnNameView(columnMapper)
    }

    def valueObject() = {
      val bd = descriptorOf[B]

      val columnMapper =
        new DynamicColumnMapper[B, CN, Array[Byte]](
          PropertyMapper[B, CN](bd(subColumnNamePropertyName), columnNameSerializer),
          new ObjectMapper[B](excludeProperties))

      createDynamicColumnNameView(columnMapper)
    }
    
    def emptyValue() = {
      val bd = descriptorOf[B]

      val columnMapper =
        new DynamicColumnMapper[B, CN, Array[Byte]](
          PropertyMapper[B, CN](bd(subColumnNamePropertyName), columnNameSerializer),
          new StaticMapper[B, Array[Byte]](Array.ofDim[Byte](0), StandardSerializers.bytesSerializer))

      createDynamicColumnNameView(columnMapper)
    }
  }
  
  def columnName(f: B => CN) = new {
    def valueProperty(valuePropertyName: String) = {
      val bd = descriptorOf[B]

      val columnMapper =
        new DynamicColumnMapper[B, CN, Any](
          new FunctionMapper[B, CN](f, columnNameSerializer),
          PropertyMapper[B](bd(valuePropertyName), serializers))

      createDynamicColumnNameView(columnMapper)
    }

    def valueObject() = {
      val columnMapper =
        new DynamicColumnMapper[B, CN, Array[Byte]](
          new FunctionMapper[B, CN](f, columnNameSerializer),
          new ObjectMapper[B](excludeProperties))

      createDynamicColumnNameView(columnMapper)
    }
    
    def emptyValue() = {
      val columnMapper =
        new DynamicColumnMapper[B, CN, Array[Byte]](
          new FunctionMapper[B, CN](f, columnNameSerializer),
          new StaticMapper[B, Array[Byte]](Array.ofDim[Byte](0), StandardSerializers.bytesSerializer))

      createDynamicColumnNameView(columnMapper)
    }
  }

  def columnName(columnName: CN) = new {
    def valueProperty(valuePropertyName: String) = {
      val bd = descriptorOf[B]

      val columnMapper =
        new StaticColumnMapper[B, CN, Any](
          columnName,
          columnNameSerializer,
          PropertyMapper[B](bd(valuePropertyName), serializers))

      createStaticColumnNameView(columnMapper)
    }

    def valueObject() = {
      val bd = descriptorOf[B]

      val columnMapper =
        new StaticColumnMapper[B, CN, Array[Byte]](
          columnName,
          columnNameSerializer,
          new ObjectMapper[B](excludeProperties))

      createStaticColumnNameView(columnMapper)
    }
    
    def emptyValue() = {
      val bd = descriptorOf[B]

      val columnMapper =
        new StaticColumnMapper[B, CN, Array[Byte]](
          columnName,
          columnNameSerializer,
          new StaticMapper[B, Array[Byte]](Array.ofDim[Byte](0), StandardSerializers.bytesSerializer))

      createStaticColumnNameView(columnMapper)
    }
  }
}