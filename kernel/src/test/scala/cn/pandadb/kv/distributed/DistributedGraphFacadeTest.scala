package cn.pandadb.kv.distributed

import cn.pandadb.kernel.distribute.DistributedGraphFacade
import cn.pandadb.kernel.store.StoredNodeWithProperty
import org.junit.{After, Before, Test}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-17 18:06
 */
class DistributedGraphFacadeTest {

  val api = new DistributedGraphFacade()

  @Before
  def init(): Unit ={
//    api.indexStore.cleanIndexes(api.nodeIndex, api.relationIndex, api.propertyIndex)
//    addNodes()
  }

  def addNodes(): Unit ={
    api.addNode(Map("name"->"glx1", "age"->11), "person", "worker")
    api.addNode(Map("name"->"glx2", "age"->12, "country"->"China"), "person", "human")
    api.addNode(Map("name"->"glx3", "age"->13), "person", "CNIC")
    api.addNode(Map("name"->"glx4", "age"->14), "person", "bass player")
    api.addNode(Map("name"->"glx5", "age"->15), "person", "man")
  }

  @Test
  def matchNodes(): Unit ={
    api.addNode(Map.empty)
    api.nodeStore.allNodes().foreach(println)
  }

  @After
  def close(): Unit ={
    api.close() // flush id to db
  }
}
