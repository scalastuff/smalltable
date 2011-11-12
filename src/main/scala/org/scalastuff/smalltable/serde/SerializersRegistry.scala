package org.scalastuff.smalltable.serde
import org.scalastuff.scalabeans.types._
import me.prettyprint.hector.api.Serializer
import me.prettyprint.cassandra.serializers.SerializerTypeInferer
import org.scalastuff.scalabeans.ManifestFactory
import org.scalastuff.scalabeans.Preamble._

/**
 * Immutable registry of serializers.
 *
 * Serializers can be mapped to a type (all fields) or certain field (by name).
 * Serializers mapped to a field always get precedence over serializers mapped to a type.
 * Some basic type checks are performed at runtime. Compile time types are captured and compared to runtime types.
 */
class SerializersRegistry(
  typeSerializers: Map[ScalaType, WrappedSerializer[_]] = Map[ScalaType, WrappedSerializer[_]](),
  fieldSerializers: Map[String, WrappedSerializer[_]] = Map[String, WrappedSerializer[_]]()) {

  def changeMapping[A](scalaType: ScalaType, serializer: Serializer[A])(implicit mf: Manifest[A]) = {
    if (!(mf >:> ManifestFactory.manifestOf(scalaType)))
      throw new RuntimeException("Cannot change type mapping: runtime type %s is not assignable to compile time type %s.".
        format(scalaType, mf))

    new SerializersRegistry(typeSerializers + (scalaType -> WrappedSerializer(serializer)), fieldSerializers)
  }

  def changeMapping[A: Manifest](fieldName: String, serializer: Serializer[A]) = {
    new SerializersRegistry(typeSerializers, fieldSerializers + (fieldName -> WrappedSerializer(serializer)))
  }

  def forProperty[A](name: String, scalaType: ScalaType)(implicit mf: Manifest[A]): Serializer[A] = {
    if (!(mf >:> ManifestFactory.manifestOf(scalaType)))
      throw new RuntimeException("Cannot get serializer for property %s: runtime type %s is not assignable to compile time type %s.".
        format(name, scalaType, mf))

    fieldSerializers.get(name) match {
      case Some(wrappedSerializer) =>
        // is it correct? I assume runtime value type is preserved during deserialization
        if (!(ManifestFactory.manifestOf(wrappedSerializer.scalaType) >:> ManifestFactory.manifestOf(scalaType)))
          throw new RuntimeException("Cannot get serializer for field %s: found serializer for type %s. This type is not assignable from required runtime type %s. Check your field serializers mappings.".
            format(name, wrappedSerializer.scalaType, scalaType))
        wrappedSerializer.ser.asInstanceOf[Serializer[A]]
      case None => forType(scalaType)
    }
  }

  def forProperty[A: Manifest](name: String): Serializer[A] = forProperty[A](name, scalaTypeOf[A])

  def forType[A: Manifest](): Serializer[A] = forType[A](scalaTypeOf[A])
  def forType[A](scalaType: ScalaType)(implicit mf: Manifest[A]): Serializer[A] = {
    if (!(mf >:> ManifestFactory.manifestOf(scalaType)))
      throw new RuntimeException("Cannot get serializer: runtime type %s is not assignable to compile time type %s.".
        format(scalaType, mf))

    typeSerializers.get(scalaType) match {
      case Some(wrappedSerializer) => wrappedSerializer.ser.asInstanceOf[Serializer[A]]
      case None => scalaType match {
        case OptionType(innerType) => StandardSerializers.optionSerializer(innerType, this).asInstanceOf[Serializer[A]]
        case EnumType(enum)        => StandardSerializers.enumStringSerializer[AnyRef](enum).asInstanceOf[Serializer[A]]
        case _                     => SerializerTypeInferer.getSerializer(scalaType.erasure)
      }
    }
  }
}

case class WrappedSerializer[A: Manifest](ser: Serializer[A]) {
  val scalaType = scalaTypeOf[A]
}