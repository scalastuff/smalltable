package org.scalastuff.smalltable

import org.junit.Assert._

trait TestUtils {
  def assertCollection[X](result: Seq[X], expected: X*) {
    assertEquals(expected.length, result.length)
    
    for (entry <- expected)
      assertTrue(result contains entry)
  }
}