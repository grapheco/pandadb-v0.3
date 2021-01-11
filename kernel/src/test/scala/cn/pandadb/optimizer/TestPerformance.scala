package cn.pandadb.optimizer

import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}
import cn.pandadb.kernel.util.Profiler
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}

import scala.collection.mutable.ArrayBuffer

object TestPerformance {
//  val dbPath = "/hdfs_data2/panda-1229/base_1B-1231"
  val dbPath = "C:\\PandaDB_rocksDB"

  val nodeStore = new NodeStoreAPI(dbPath)
  val relationStore = new RelationStoreAPI(dbPath)
  val indexStore = new IndexStoreAPI(dbPath)
  val statistics = new Statistics(dbPath)
  var graphFacade = new GraphFacadeWithPPD(
    nodeStore,
    relationStore,
    indexStore,
    statistics,
    {}
  )

  def main(args: Array[String]): Unit = {
    heatDB()
//    memTest()
//    memTestApi()

    relationCypherTest()
    relationApiTest()
//    cypherWithoutIndex()
//    createIndex()
//    cypherWithIndex()
//    APIWithoutIndex()
//    APIWithIndex()

    graphFacade.close()

  }

  def heatDB(): Unit = {
//    graphFacade.cypher("match (n) return n limit 1000")
//    graphFacade.cypher("match (n:label0) return n limit 1000")
//    graphFacade.cypher("match (n:label2) where n.flag = false return n limit 1000")
//    graphFacade.cypher("match(n:label0) where n.idStr='ha' return n")
//    graphFacade.cypher("match (n:label0) where n.flag = false and n.idStr='ea' and n.id_p=40 return n")
//    graphFacade.cypher("match(n:label1) where n.name = 'Alice Panda' return n")

    graphFacade.cypher("match (n:label0)-[r]->(m:label1) return r limit 10000")
    graphFacade.cypher("match (n:label2)-[r1]->(m:label3)-[r2]->(p:label4) return r2  limit 10000")
    graphFacade.cypher("match (n:label5)-[r1]->(m:label6)-[r2]->(p:label7)-[r3]->(q:label8) return r3  limit 10000")
    graphFacade.cypher("match (n:label0)-[r:type0]->(m:label2) return r  limit 10000")
    graphFacade.cypher("match (n:label2)-[r]->(m:label4) return r  limit 10000")
//    graphFacade.cypher("match (n:label3)-[r:type1]->(m:label6) return r limit 10000")
//    graphFacade.cypher("match (n:label4)-[r]->(m:label8) return r limit 10000")


  }
  def memTest(): Unit ={
    Profiler.timing(
      {
        val res = graphFacade.cypher("match (n) where n.idStr='ha' return n")
        println(res.records.size)
      }
    )
  }
  def memTestApi(): Unit ={
    val pid = nodeStore.getPropertyKeyId("idStr")
    val lst = ArrayBuffer[StoredNodeWithProperty]()
    Profiler.timing(
      {
        val iter = nodeStore.allNodes()
        var count = 0
        while (iter.hasNext){
          val record = iter.next()
          if (record.properties.getOrElse(pid, "") == "ha"){
            count += 1
          }
        }
        println(count)
      }
    )
  }

  def relationApiTest(): Unit ={
    Profiler.timing({
      println("Test preheat")
      val label0 = nodeStore.getLabelId("label8")
      val label1 = nodeStore.getLabelId("label9")
      val limit = 10000

      val res = nodeStore
        //.getNodeIdsByLabel(label0)
        .getNodesByLabel(label0).map(mapNode(_)).map(_.id)
        .flatMap(relationStore.findOutRelations)
        .filter(rel =>nodeStore.hasLabel(rel.to, label1))
        .take(limit)
        .map(mapRelation)
      println(res.size)
    })

    print("match (n:label0)-[r]->(m:label1) return r limit 10000   ")
    Profiler.timing({
      val label0 = nodeStore.getLabelId("label0")
      val label1 = nodeStore.getLabelId("label1")
      val limit = 10000

      val res = nodeStore
        //.getNodeIdsByLabel(label0)
          .getNodesByLabel(label0).map(mapNode(_)).map(_.id)
        .flatMap(relationStore.findOutRelations)
        .filter(rel =>nodeStore.hasLabel(rel.to, label1))
        .take(limit)
        .map(mapRelation)
      println(res.size)
    })

    print("match (n:label2)-[r1]->(m:label3)-[r2]->(p:label4) return r2  limit 10000    ")
    Profiler.timing({
      val label0 = nodeStore.getLabelId("label2")
      val label1 = nodeStore.getLabelId("label3")
      val label2 = nodeStore.getLabelId("label4")
      val limit = 10000

      val res = nodeStore
        .getNodeIdsByLabel(label0)
        .flatMap(relationStore.findOutRelations)
        .map(_.to)
        .filter(nodeId =>nodeStore.hasLabel(nodeId, label1))
        .flatMap(relationStore.findOutRelations)
        .filter(rel =>nodeStore.hasLabel(rel.to, label2))
        .take(limit)
        .map(mapRelation)

      println(res.size)
    })

    print("match (n:label5)-[r1]->(m:label6)-[r2]->(p:label7)-[r3]->(q:label8) return r3  limit 10000    ")
    Profiler.timing({
      val label0 = nodeStore.getLabelId("label5")
      val label1 = nodeStore.getLabelId("label6")
      val label2 = nodeStore.getLabelId("label7")
      val label3 = nodeStore.getLabelId("label8")
      val limit = 10000



      val res = nodeStore
        .getNodeIdsByLabel(label0)
        .flatMap(relationStore.findOutRelations)
        .map(_.to)
        .filter(nodeId =>nodeStore.hasLabel(nodeId, label1))
        .flatMap(relationStore.findOutRelations)
        .map(_.to)
        .filter(nodeId =>nodeStore.hasLabel(nodeId, label2))
        .flatMap(relationStore.findOutRelations)
        .filter(rel =>nodeStore.hasLabel(rel.to, label3))
        .take(limit)
        .map(mapRelation)
      println(res.length)
    })

    print("match (n:label0)-[r:type0]->(m:label2) return r  limit 10000    ")
    Profiler.timing({
      val label0 = nodeStore.getLabelId("label0")
      val label1 = nodeStore.getLabelId("label2")
      val type0  = relationStore.getRelationTypeId("type0")
      val limit = 10000

      val res = nodeStore
        .getNodeIdsByLabel(label0)
        .flatMap(nodeId => relationStore.findOutRelations(nodeId, type0))
        .filter(rel =>nodeStore.hasLabel(rel.to, label1))
        .take(limit)
        .map(mapRelation)
      println(res.length)
    })

    print("match (n:label2)-[r]->(m:label4) return r limit 10000     ")
    Profiler.timing({
      val label0 = nodeStore.getLabelId("label2")
      val label1 = nodeStore.getLabelId("label4")
      val limit = 10000

      val res = nodeStore
        .getNodeIdsByLabel(label0)
        .flatMap(relationStore.findOutRelations)
        .filter(rel =>nodeStore.hasLabel(rel.to, label1))
        .take(limit)
        .map(mapRelation)
      println(res.length)
    })
  }

  def relationCypherTest(): Unit ={
    val cyphers = List(
      "match (n:label8)-[r]->(m:label9) return r limit 10000",
      "match (n:label0)-[r]->(m:label1) return r limit 10000",
      "match (n:label2)-[r1]->(m:label3)-[r2]->(p:label4) return r2  limit 10000",
      "match (n:label5)-[r1]->(m:label6)-[r2]->(p:label7)-[r3]->(q:label8) return r3  limit 10000",
      "match (n:label0)-[r:type0]->(m:label2) return r  limit 10000",
      "match (n:label2)-[r]->(m:label4) return r limit 10000",
      // "match (n:label0)-[r:type1]->(m:label1) return r limit 10000",
//      "match (n:label3)-[r]->(m:label6) return r limit 10000",
//      "match (n:label3)-[r:type1]->(m:label6) return r limit 10000",
//      "match (n:label4)-[r]->(m:label8) return r limit 10000"
    )
    cyphers.foreach(c =>{
      Profiler.timing({
        val res = graphFacade.cypher(c)
        println(res.records.size, "CypherQuery", c)
        //println(res.getRecords.get.show, "~~~~")
        //println(c)
      })
    })
  }

  def cypherWithoutIndex(): Unit = {
    println("============================cypher without Index Start...========================================================")
    val cyphers = List(
      "match (n) return n limit 1000",
      "match (n:label0) return n limit 1000",
      "match (n:label2) where n.flag = false return n limit 1000",
      "match(n:label0) where n.idStr='ha' return n",
      "match (n:label0) where n.flag = false and n.idStr='ea' and n.id_p=40 return n",
      "match(n:label1) where n.name = 'Alice Panda' return n"
    )
    cyphers.foreach(c =>{
      Profiler.timing({
        val res = graphFacade.cypher(c)
        println(res.records.size)
      })
    })
    println("============================cypher without Index END...========================================================")

  }

  def createIndex(): Unit = {
    println("============================create Index Start...========================================================")
    graphFacade.createIndexOnNode("label0", Set("flag"))
    graphFacade.createIndexOnNode("label0", Set("idStr"))
    graphFacade.createIndexOnNode("label0", Set("id_p"))
    graphFacade.createIndexOnNode("label2", Set("flag"))
    graphFacade.createIndexOnNode("label1", Set("name"))

    println("============================create Index END...========================================================")

  }

  def cypherWithIndex(): Unit = {
    println("============================cypher with Index Start...========================================================")
    val cyphers = List(
//      "match (n) return n limit 1000",
//      "match (n:label0) return n limit 1000",
      "match (n:label2) where n.flag = false return n limit 1000",
      "match(n:label0) where n.idStr='ha' return n",
      "match (n:label0) where n.flag = false and n.idStr='ea' and n.id_p=40 return n",
      "match(n:label1) where n.name = 'Alice Panda' return n "
    )

    cyphers.foreach(c =>{
      Profiler.timing({
        val res = graphFacade.cypher(c)
        println(res.records.size)
      })
    })
    println("============================cypher with Index END...========================================================")

  }

  def APIWithoutIndex(): Unit = {
    println("============================API without Index Start...========================================================")

    Profiler.timing({
      val label = nodeStore.getLabelId("label1")
      val prop = nodeStore.getPropertyKeyId("name")
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == "Alice Panda") {
          res += node
        }
      }
    })

    Profiler.timing({
      val iter = nodeStore.allNodes().take(100000).toArray
      iter.length
    })

    Profiler.timing({
      val label = nodeStore.getLabelId("label0")
      val iter = nodeStore.getNodesByLabel(label)
      for (i <- 1 to 1000) {
        iter.next().properties
      }
    })

    Profiler.timing({
      val label = nodeStore.getLabelId("label2")
      val prop = nodeStore.getPropertyKeyId("flag")
      var count = 0
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext && count < 1000) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == false) {
          res += node
          count += 1
        }
      }
    })

    Profiler.timing({
      val label = nodeStore.getLabelId("label0")
      val prop = nodeStore.getPropertyKeyId("idStr")
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == "ha") {
          res += node
        }
      }
    })

    Profiler.timing({
      val label = nodeStore.getLabelId("label0")
      val propIdStr = nodeStore.getPropertyKeyId("idStr")
      val propId_p = nodeStore.getPropertyKeyId("id_p")
      val propFlag = nodeStore.getPropertyKeyId("flag")
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext) {
        val node = nodes.next()
        val ps = node.properties
        if (ps.getOrElse(propIdStr, null) == "ea" &&
          ps.getOrElse(propFlag, null) == false &&
          ps.getOrElse(propId_p, null) == 40
        ) {
          res += node
        }
      }
    })

    Profiler.timing({
      val label = nodeStore.getLabelId("label1")
      val prop = nodeStore.getPropertyKeyId("name")
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == "Alice Panda") {
          res += node
        }
      }
    })
    println("============================API without Index END...========================================================")

  }

  def APIWithIndex(): Unit = {
    println("============================API with Index start...========================================================")

    Profiler.timing({
      val label = nodeStore.getLabelId("label2")
      val prop = nodeStore.getPropertyKeyId("flag")
      val indexId = indexStore.getIndexId(label, Array(prop)).get
      var count = 0
      val nodes = indexStore.find(indexId, false)
      val res = ArrayBuffer[StoredNodeWithProperty]()

      while (nodes.hasNext && count < 1000) {
        val nodeId = nodes.next()
        val node = nodeStore.getNodeById(nodeId).get
        res += node
        count += 1
      }
    })
    //
    Profiler.timing({
      val label = nodeStore.getLabelId("label0")
      val prop = nodeStore.getPropertyKeyId("idStr")
      val indexId = indexStore.getIndexId(label, Array(prop)).get
      val nodes = indexStore.find(indexId, "ha")
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext) {
        val nodeId = nodes.next()
        val node = nodeStore.getNodeById(nodeId).get
        res += node
      }
    })

    Profiler.timing({
      val label = nodeStore.getLabelId("label0")
      val propIdStr = nodeStore.getPropertyKeyId("idStr")
      val propId_p = nodeStore.getPropertyKeyId("id_p")
      val propFlag = nodeStore.getPropertyKeyId("flag")

      val indexId1 = indexStore.getIndexId(label, Array(propIdStr)).get
      val indexId2 = indexStore.getIndexId(label, Array(propId_p)).get
      val indexId3 = indexStore.getIndexId(label, Array(propFlag)).get

      val iter1 = indexStore.find(indexId1, "ea")
      val iter2 = indexStore.find(indexId2, 40)
      val iter3 = indexStore.find(indexId3, false)

      while (iter1.hasNext) {
        val nodeId = iter1.next()
        nodeStore.getNodeById(nodeId).get.properties
      }
    })

    Profiler.timing({
      val label = nodeStore.getLabelId("label1")
      val prop = nodeStore.getPropertyKeyId("name")
      val indexId = indexStore.getIndexId(label, Array(prop)).get
      val iter = indexStore.find(indexId, "Alice Panda")
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (iter.hasNext) {
        val nodeId = iter.next()
        val node = nodeStore.getNodeById(nodeId).get
        res += node
      }
    })
    println("============================API with Index End...========================================================")

  }

  protected def mapRelation(rel: StoredRelation): Relationship[Long] = {
    new Relationship[Long] {
      override type I = this.type

      override def id: Long = rel.id

      override def startId: Long = rel.from

      override def endId: Long = rel.to

      override def relType: String = relationStore.getRelationTypeName(rel.typeId).get

      override def copy(id: Long, source: Long, target: Long, relType: String, properties: CypherMap): this.type = ???

      override def properties: CypherMap = {
        var props: Map[String, Any] = Map.empty[String, Any]
        rel match {
          case rel: StoredRelationWithProperty =>
            props = rel.properties.map(kv => (relationStore.getRelationTypeName(kv._1).getOrElse("unknown"), kv._2))
          case _ =>
        }
        CypherMap(props.toSeq: _*)
      }
    }
  }

  protected def mapNode(node: StoredNode): Node[Long] = {
    new Node[Long] {
      override type I = this.type

      override def id: Long = node.id

      override def labels: Set[String] = node.labelIds.toSet.map((id: Int) => nodeStore.getLabelName(id).get)

      override def copy(id: Long, labels: Set[String], properties: CypherValue.CypherMap): this.type = ???

      override def properties: CypherMap = {
        var props: Map[String, Any] = Map.empty[String, Any]
        node match {
          case node: StoredNodeWithProperty =>
            props = node.properties.map {
              kv =>
                (nodeStore.getPropertyKeyName(kv._1).get, kv._2)
            }
          case _ =>
            val n = nodeStore.getNodeById(node.id)
            props = n.asInstanceOf[StoredNodeWithProperty].properties.map {
              kv =>
                (nodeStore.getPropertyKeyName(kv._1).get, kv._2)
            }
        }
        CypherMap(props.toSeq: _*)
      }
    }
  }
}