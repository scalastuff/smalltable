package org.scalastuff.smalltable

import org.scalastuff.smalltable._
import org.junit._
import org.junit.Assert._

class SuperColumnViewTest extends TestUtils {
  import test._
  
  object WhateverDatabase extends Database("Whatever") {
    override def checkConfMode = conf.CheckConfigurationMode.DropCreate()

    def configurationChecks = check(Prices.columnFamilyDefinition) :: Nil
  }
  
  object Prices extends SuperColumnFamily("Prices") {
    type KeyValue = String
    type SuperColumnName = String
    type ColumnName = String
    
    val view = materializedView[OHLC].rowKeyProperty("day").superColumnNameProperty("ticker").staticColumnNames()
  }
  
  @Test
  def testStorage() {
    val expected = OHLC("ASML", "20100101", "22.0", "22.5", "21.34", "22.3", 12345)
    WhateverDatabase.execute(Prices.view.put(expected))    
    
    val retrieved = WhateverDatabase.execute(Prices.view.get("20100101", "ASML"))    
    assertEquals(Some(expected), retrieved)
    
    assertEquals(None, WhateverDatabase.execute(Prices.view.get("20100101", "KPN")))
    assertEquals(None, WhateverDatabase.execute(Prices.view.get("20110101", "ASML")))
  }
  
  @Test
  def testSlice() {
    val expected1 = OHLC("ASML", "20100101", "22.0", "22.5", "21.34", "22.3", 12345)
    val expected2 = OHLC("KPN", "20100101", "12.0", "12.5", "11.34", "12.3", 78945)
    val unexpected = OHLC("ASML", "20100102", "23.0", "23.5", "22.34", "23.3", 12346)
    WhateverDatabase.execute(Prices.view.put(expected1), Prices.view.put(expected2), Prices.view.put(unexpected))    
       
    assertCollection(WhateverDatabase.execute(Prices.view.getSlice("20100101")), expected1, expected2)
    
    assertEquals(0, WhateverDatabase.execute(Prices.view.getSlice("20110101")).length)
  }
  
  @Before
  def beforeTest() {
    WhateverDatabase.truncate(Prices)
  }
}

package test {
  case class OHLC(ticker: String, day: String, open: String, high: String, low: String, close: String, volume: Long)
}