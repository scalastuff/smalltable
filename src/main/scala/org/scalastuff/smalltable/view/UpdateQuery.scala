package org.scalastuff.smalltable.view
import me.prettyprint.hector.api.mutation.Mutator
import me.prettyprint.hector.api.Serializer

case class UpdateQuery[K](rowKeySerializer: Serializer[K], mutation: Mutator[K] => _)