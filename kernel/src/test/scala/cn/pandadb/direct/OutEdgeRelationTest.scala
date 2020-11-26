package cn.pandadb.direct

import cn.pandadb.kernel.direct.OutEdgeRelationIndex
import org.junit.{Assert, Test}

import scala.util.Random


class OutEdgeRelationTest {

  @Test
  def testForStoreOneRelationship: Unit = {
    val store = new OutEdgeRelationIndex(10000)
    store.addRelationship(1,1,1)
    Assert.assertArrayEquals(Array(1L), store.getOutEdges(1,1).map(record=>record.endNode).toArray)
  }

  @Test
  def testForOneFromNodeMultiToNodes: Unit = {
    val store = new OutEdgeRelationIndex(10000)
    val endNodes = scala.collection.mutable.Set[Long]()
    for(i<- 0 to 99) endNodes.add(math.abs(Random.nextLong())+1)
    endNodes.foreach(n => store.addRelationship(1, n, 1))
    Assert.assertArrayEquals(endNodes.toArray.sorted, store.getOutEdges(1,1).map(record=>record.endNode).toArray.sorted)
  }

  @Test
  def testForMultiFromNodeMultiToNodes: Unit = {
    val store = new OutEdgeRelationIndex(10000)
    val endNodes = scala.collection.mutable.Set[Long]()

    for(i<- 0 to 99) endNodes.add(math.abs(Random.nextLong())+1)
    endNodes.foreach(n => store.addRelationship(1, n, 1))
    Assert.assertArrayEquals(endNodes.toArray.sorted, store.getOutEdges(1,1).map(record=>record.endNode).toArray.sorted)

    for(i<- 0 to 99) endNodes.add(math.abs(Random.nextLong())+1)
    endNodes.foreach(n => store.addRelationship(2, n, 1))
    Assert.assertArrayEquals(endNodes.toArray.sorted, store.getOutEdges(2,1).map(record=>record.endNode).toArray.sorted)

    for(i<- 0 to 99) endNodes.add(math.abs(Random.nextLong())+1)
    endNodes.foreach(n => store.addRelationship(10002, n, 1))
    Assert.assertArrayEquals(endNodes.toArray.sorted, store.getOutEdges(10002,1).map(record=>record.endNode).toArray.sorted)

    for(i<- 0 to 99) endNodes.add(math.abs(Random.nextLong())+1)
    endNodes.foreach(n => store.addRelationship(10005, n, 1))
    Assert.assertArrayEquals(endNodes.toArray.sorted, store.getOutEdges(10005,1).map(record=>record.endNode).toArray.sorted)
  }

  @Test
  def testForMultiLabel: Unit = {
    val store = new OutEdgeRelationIndex(10000)
    val endNodes = scala.collection.mutable.Set[Long]()
    for(i<- 0 to 99) endNodes.add(math.abs(Random.nextLong())+1)

    endNodes.foreach(n => store.addRelationship(1, n, 1))
    Assert.assertArrayEquals(endNodes.toArray.sorted, store.getOutEdges(1,1).map(record=>record.endNode).toArray)

    for(i<- 0 to 99) endNodes.add(math.abs(Random.nextLong())+1)
    endNodes.foreach(n => store.addRelationship(2, n, 2))
    Assert.assertArrayEquals(endNodes.toArray.sorted, store.getOutEdges(2,2).map(record=>record.endNode).toArray)

    for(i<- 0 to 99) endNodes.add(math.abs(Random.nextLong())+1)
    endNodes.foreach(n => store.addRelationship(10002, n, 3))
    Assert.assertArrayEquals(endNodes.toArray.sorted, store.getOutEdges(10002,3).map(record=>record.endNode).toArray)

    for(i<- 0 to 99) endNodes.add(math.abs(Random.nextLong())+1)
    endNodes.foreach(n => store.addRelationship(10005, n, 1))
    Assert.assertArrayEquals(endNodes.toArray.sorted, store.getOutEdges(10005,1).map(record=>record.endNode).toArray)
  }

  @Test
  def testForInvalidId() = {
    val store = new OutEdgeRelationIndex(10000)
    try {
      store.addRelationship(0,1,1)
    } catch {
      case ex: Error => Assert.assertTrue(ex.isInstanceOf[java.lang.AssertionError])
    }
    try {
      store.addRelationship(-1,1,1)
    } catch {
      case ex: Error => Assert.assertTrue(ex.isInstanceOf[java.lang.AssertionError])
    }
    try {
      store.addRelationship(1,0,1)
    } catch {
      case ex: Error => Assert.assertTrue(ex.isInstanceOf[java.lang.AssertionError])
    }
    try {
      store.addRelationship(1,-1,1)
    } catch {
      case ex: Error => Assert.assertTrue(ex.isInstanceOf[java.lang.AssertionError])
    }
    try {
      store.addRelationship(1,1,0)
    } catch {
      case ex: Error => Assert.assertTrue(ex.isInstanceOf[java.lang.AssertionError])
    }
    try {
      store.addRelationship(1,1,-1)
    } catch {
      case ex: Error => Assert.assertTrue(ex.isInstanceOf[java.lang.AssertionError])
    }
  }

  @Test
  def testForDeleteRelationship(): Unit = {
    val store = new OutEdgeRelationIndex(10000)
    val endNodes = scala.collection.mutable.Set[Long]()

    for(i<- 0 to 99) endNodes.add(math.abs(Random.nextLong())+1)
    endNodes.foreach(n => store.addRelationship(1, n, 1))
    Assert.assertArrayEquals(endNodes.toArray.sorted, store.getOutEdges(1,1).map(record=>record.endNode).toArray)

    val deleteEndNodes = endNodes.drop(50)
    deleteEndNodes.foreach(n => endNodes.remove(n))
    deleteEndNodes.foreach(n => store.deleteRelation(1, n, 1))
    Assert.assertArrayEquals(endNodes.toArray.sorted, store.getOutEdges(1,1).map(record=>record.endNode).toArray)

    endNodes.foreach(n => store.deleteRelation(1, n, 1))
    endNodes.clear()
    Assert.assertArrayEquals(endNodes.toArray.sorted, store.getOutEdges(1,1).map(record=>record.endNode).toArray)

  }

}
