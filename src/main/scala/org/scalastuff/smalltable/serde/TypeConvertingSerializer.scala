package org.scalastuff.smalltable.serde

import me.prettyprint.hector.api.Serializer
import me.prettyprint.cassandra.serializers.AbstractSerializer
import java.nio.ByteBuffer

class TypeConvertingSerializer[A, B](to: A => B, from: B => A)(implicit baseSerializer: Serializer[B]) extends AbstractSerializer[A] {
	def toByteBuffer(v: A) = baseSerializer.toByteBuffer(to(v))
    def fromByteBuffer(buffer: ByteBuffer): A = from(baseSerializer.fromByteBuffer(buffer))
    override def getComparatorType = baseSerializer.getComparatorType
}