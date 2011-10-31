package org.scalastuff.smalltable

import org.scalastuff.smalltable._
import org.junit._
import org.junit.Assert._

/**
 * This is integration test, requiring running Cassandra instance on localhost:9160
 */
class RowViewTest {
  import test._
  
  object WhateverDatabase extends Database("Whatever") {
    override def checkConfMode = conf.CheckConfigurationMode.Update()

    def configurationChecks = check(Jobs.columnFamilyDefinition) :: Nil
  }
  
  object Jobs extends ColumnFamily("jobs") {
    type KeyValue = Long
    type ColumnName = String
    
    val jobInfoView = materializedView[JobInfo].rowKeyProperty("jobId").
    	staticColumnNames(exclude = List("haha"), rename = List("jobName" -> "n", "description" -> "d"), index = List("d"))
  }
  
  @Test
  def testStorage() {    
    val expected = JobInfo(1, "test job", "descr")
    WhateverDatabase.execute(Jobs.jobInfoView.put(expected))
    val retrieved = WhateverDatabase.execute(Jobs.jobInfoView.get(1))
    assertEquals(Some(expected), retrieved)
    
    assertEquals(None, WhateverDatabase.execute(Jobs.jobInfoView.get(2)))
  }
  
  @Test
  def testIndex() {    
    val expected1 = JobInfo(1, "test job #1", "descr")
    val expected2 = JobInfo(2, "test job #2", "descr")
    val unexpected = JobInfo(3, "test job #3", "other")
    
    WhateverDatabase.execute(
        Jobs.jobInfoView.put(expected1),
        Jobs.jobInfoView.put(expected2),
        Jobs.jobInfoView.put(unexpected))
    val retrieved = WhateverDatabase.execute(Jobs.jobInfoView.select("d='descr'"))
    assertEquals(2, retrieved.length)
    assertTrue(retrieved contains expected1)
    assertTrue(retrieved contains expected2)
  }
  
  @Before
  def beforeTest() {
    WhateverDatabase.truncate(Jobs)
  }
}

package test {
  case class JobInfo(jobId: Long, jobName: String, description: String, haha: Long = 0)
}