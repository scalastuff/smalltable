package org.scalastuff.smalltable.serde
import me.prettyprint.cassandra.serializers.BytesArraySerializer
import org.scalastuff.scalabeans.Enum
import org.scalastuff.scalabeans.types._
import me.prettyprint.cassandra.serializers._
import java.nio.ByteBuffer
import org.scalastuff.scalabeans.Preamble._
import org.scalastuff.scalabeans.ManifestFactory
import me.prettyprint.hector.api.Serializer

object StandardSerializers {
  val bytesSerializer = BytesArraySerializer.get()

  val stringSerializer = StringSerializer.get()
  val dateSerializer = DateSerializer.get()
  val uuidSerializer = UUIDSerializer.get()
  val timeUuidSerializer = TimeUUIDSerializer.get()

  val byteSerializer = new AbstractSerializer[Byte] {
    def toByteBuffer(v: Byte) = {
      val bb = ByteBuffer.allocate(1)
      bb.put(v)
      bb.rewind()
      bb
    }
    def fromByteBuffer(buffer: ByteBuffer): Byte = buffer.get()
    override def fromBytes(buffer: Array[Byte]): Byte = buffer(0)
  }

  val shortSerializer = new AbstractSerializer[Short] {
    val wrapped = ShortSerializer.get()

    override def toByteBuffer(v: Short) = wrapped.toByteBuffer(v)
    override def fromByteBuffer(buffer: ByteBuffer): Short = wrapped.fromByteBuffer(buffer).shortValue
    override def fromBytes(buffer: Array[Byte]): Short = wrapped.fromBytes(buffer).shortValue
    override def getComparatorType = wrapped.getComparatorType
  }
  val intSerializer = new AbstractSerializer[Int] {
    val wrapped = IntegerSerializer.get()

    override def toByteBuffer(i: Int) = wrapped.toByteBuffer(i)
    override def fromByteBuffer(buffer: ByteBuffer): Int = wrapped.fromByteBuffer(buffer).intValue
    override def fromBytes(buffer: Array[Byte]): Int = wrapped.fromBytes(buffer).intValue
    override def getComparatorType = wrapped.getComparatorType
  }
  val longSerializer = new AbstractSerializer[Long] {
    val wrapped = LongSerializer.get()

    override def toByteBuffer(v: Long) = wrapped.toByteBuffer(v)
    override def fromByteBuffer(buffer: ByteBuffer): Long = wrapped.fromByteBuffer(buffer).longValue
    override def fromBytes(buffer: Array[Byte]): Long = wrapped.fromBytes(buffer).longValue
    override def getComparatorType = wrapped.getComparatorType
  }
  val doubleSerializer = new AbstractSerializer[Double] {
    val wrapped = DoubleSerializer.get()

    override def toByteBuffer(v: Double) = wrapped.toByteBuffer(v)
    override def fromByteBuffer(buffer: ByteBuffer): Double = wrapped.fromByteBuffer(buffer).doubleValue
    override def fromBytes(buffer: Array[Byte]): Double = wrapped.fromBytes(buffer).doubleValue
    override def getComparatorType = wrapped.getComparatorType
  }
  val floatSerializer = new AbstractSerializer[Float] {
    val wrapped = FloatSerializer.get()

    override def toByteBuffer(v: Float) = wrapped.toByteBuffer(v)
    override def fromByteBuffer(buffer: ByteBuffer): Float = wrapped.fromByteBuffer(buffer).floatValue
    override def fromBytes(buffer: Array[Byte]): Float = wrapped.fromBytes(buffer).floatValue
    override def getComparatorType = wrapped.getComparatorType
  }
  val booleanSerializer = new TypeConvertingSerializer[Boolean, java.lang.Boolean]((v => v), _.booleanValue)(BooleanSerializer.get())  

  val bigDecimalStringSerializer = new TypeConvertingSerializer[BigDecimal, String](_.toString, BigDecimal(_))(stringSerializer)
  
  val bigDecimalBytesSerializer = new AbstractSerializer[BigDecimal] {

    /**
     * The bytes of the ByteBuffer are made up of 4 bytes of int containing the scale
     * followed by the n bytes it takes to store a BigInteger.
     */
    def toByteBuffer(v: BigDecimal) = {
      if (v eq null) null
      else {
        val bi = v.bigDecimal.unscaledValue
        val scale = v.scale
        val bibytes = bi.toByteArray()
        val sbytes = ByteBuffer.allocate(4).putInt(0, scale).array;
        val bytes = Array.ofDim[Byte](bibytes.length + 4);

        var i = 0
        while (i < 4) {
          bytes(i) = sbytes(i)
          i += 1
        }
        while (i < bibytes.length + 4) {
          bytes(i) = bibytes(i - 4)
          i += 1
        }

        ByteBuffer.wrap(bytes)
      }
    }
    def fromByteBuffer(buffer: ByteBuffer): BigDecimal = {
      if (buffer == null) return null;

      val scale = buffer.getInt
      val bibytes = Array.ofDim[Byte](buffer.remaining())
      buffer.get(bibytes, 0, buffer.remaining());
      val bi = BigInt(bibytes);

      return BigDecimal(bi, scale);
    }
    //override def getComparatorType = TODO: add Decimal comparator type when it is implemented
  }

  //TODO: collection serializer
  //TODO: array serializer
  def optionSerializer(innerType: ScalaType, registry: SerializersRegistry) = {
    new AbstractSerializer[Option[Any]] {
      val innerTypeSerializer = registry.forType[Any](innerType)
      def toByteBuffer(v: Option[Any]) = v match {
        case Some(x) => innerTypeSerializer.toByteBuffer(x)
        case None    => null
      }

      def fromByteBuffer(buffer: ByteBuffer): Option[Any] = {
        if (buffer == null) None
        else Some(innerTypeSerializer.fromByteBuffer(buffer))
      }

      override def getComparatorType() = innerTypeSerializer.getComparatorType
    }
  }
  
  def enumStringSerializer[A <: AnyRef](enum: Enum[A]) = new TypeConvertingSerializer[A, String](enum.nameOf(_), enum.valueOf(_).get)(stringSerializer)
  def enumOrdinalSerializer[A <: AnyRef](enum: Enum[A]) = new TypeConvertingSerializer[A, Int](enum.ordinalOf(_), enum.valueOf(_).get)(intSerializer)
  
  val registry =
    new SerializersRegistry(
      Map(
        ByteType -> WrappedSerializer(byteSerializer),
        ShortType -> WrappedSerializer(shortSerializer),
        IntType -> WrappedSerializer(intSerializer),
        LongType -> WrappedSerializer(LongSerializer.get()),
        DoubleType -> WrappedSerializer(doubleSerializer),
        FloatType -> WrappedSerializer(floatSerializer),
        BooleanType -> WrappedSerializer(booleanSerializer),
        BigDecimalType -> WrappedSerializer(bigDecimalStringSerializer)))
}