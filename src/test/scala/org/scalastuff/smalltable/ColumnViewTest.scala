package org.scalastuff.smalltable

import org.scalastuff.smalltable._
import org.junit._
import org.junit.Assert._

/**
 * This is integration test, requiring running Cassandra instance on localhost:9160
 */
class ColumnViewTest extends TestUtils {
  import test._
  
  object WhateverDatabase extends Database("Whatever") {
    override def checkConfMode = conf.CheckConfigurationMode.Update()

    def configurationChecks = check(Jobs.columnFamilyDefinition) :: Nil
  }
  
  object Jobs extends ColumnFamily("jobs") {
    type KeyValue = Long
    type ColumnName = String
    
    val jobRunView = materializedView[JobRun].rowKeyProperty("jobId").columnNameProperty("started").valueProperty("status")
  }
  
  @Test
  def testStorage {
    val expected = JobRun(1, "now", 1)
    WhateverDatabase.execute(Jobs.jobRunView.put(expected))
    val retrieved = WhateverDatabase.execute(Jobs.jobRunView.get(1, "now"))
    assertEquals(Some(expected), retrieved)
    
    assertEquals(None, WhateverDatabase.execute(Jobs.jobRunView.get(2, "now")))
    assertEquals(None, WhateverDatabase.execute(Jobs.jobRunView.get(1, "yesterday")))
  }
  
  @Test
  def testSlice {
    val expected1 = JobRun(1, "now", 1)
    val expected2 = JobRun(1, "later", 2)
    val unexpected = JobRun(2, "now", 3)
    
    WhateverDatabase.execute(Jobs.jobRunView.put(expected1), Jobs.jobRunView.put(expected2), Jobs.jobRunView.put(unexpected))
    assertCollection(WhateverDatabase.execute(Jobs.jobRunView.getSlice(1)), expected1, expected2)
    assertCollection(WhateverDatabase.execute(Jobs.jobRunView.getSlice(1, Some("later"))), expected1, expected2)
    assertCollection(WhateverDatabase.execute(Jobs.jobRunView.getSlice(1, Some("later"), Some("now"))), expected1, expected2)
    assertCollection(WhateverDatabase.execute(Jobs.jobRunView.getSlice(1, Some("l"), Some("n"))), expected2)
    assertCollection(WhateverDatabase.execute(Jobs.jobRunView.getSlice(1, "now", "later")), expected1, expected2)
    
    assertEquals(0, WhateverDatabase.execute(Jobs.jobRunView.getSlice(10)).length)
  }
  
  @Before
  def beforeTest() {
    WhateverDatabase.truncate(Jobs)
  }
  
  
}

package test {
  case class JobRun(jobId: Long, started: String, status: Long)
}