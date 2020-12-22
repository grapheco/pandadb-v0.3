package cn.pandadb.kernel.kv

import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.kv.name.NameStore
import cn.pandadb.kernel.optimizer.PandaCypherSession
import cn.pandadb.kernel.store._
import org.apache.logging.log4j.scala.Logging
import org.opencypher.lynx.{LynxSession, PropertyGraphScan}
import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship}

class GraphFacadeWithPPD(
                          nodeLabelStore: NameStore,
                          relLabelStore: NameStore,
                          propertyNameStore: NameStore,
                          nodeIdGen: FileBasedIdGen,
                          relIdGen: FileBasedIdGen,
                          graphStore: RocksDBGraphAPI,
                          onClose: => Unit
                 ) extends Logging with GraphService {

  private val propertyGraph = new PandaCypherSession().createPropertyGraph(new PandaPropertyGraphScanImpl(
      nodeLabelStore, relLabelStore, propertyNameStore, nodeIdGen, relIdGen, graphStore
  ) )


  init()

  override def cypher(query: String, parameters: Map[String, Any] = Map.empty): CypherResult = {
    propertyGraph.cypher(query, CypherMap(parameters.toSeq: _*))
  }

  override def close(): Unit = {
    nodeIdGen.flush()
    relIdGen.flush()
    graphStore.close()
    onClose
  }

  override def addNode(nodeProps: Map[String, Any], labels: String*): this.type = {
    val nodeId = nodeIdGen.nextId()
    val labelIds = nodeLabelStore.ids(labels.toSet).toArray
    //TODO: string name to id
    val props = nodeProps.map{
      v => ( propertyNameStore.id(v._1),v._2)
    }
    graphStore.addNode(new StoredNodeWithProperty(nodeId, labelIds, props))

    this
  }

  def addNode2(nodeProps: Map[String, Any], labels: String*): Long = {
    val nodeId = nodeIdGen.nextId()
    val labelIds = nodeLabelStore.ids(labels.toSet).toArray
    //TODO: string name to id
    val props = nodeProps.map{
      v => ( propertyNameStore.id(v._1),v._2)
    }
    graphStore.addNode(new StoredNodeWithProperty(nodeId, labelIds, props))
    nodeId
  }

  override def addRelation(label: String, from: Long, to: Long, relProps: Map[String, Any]): this.type = {
    val rid = relIdGen.nextId()
    val labelId = relLabelStore.id(label)
    val rel = StoredRelation(rid, from, to, labelId)
    //TODO: transaction safe
    graphStore.addRelation(rel)
    this
  }

  override def deleteNode(id: Id): this.type = {
    graphStore.deleteNode(id)
    this
  }

  override def deleteRelation(id: Id): this.type = {
    graphStore.deleteRelation(id)
    this
  }

  def allNodes(): Iterable[StoredNode] = {
    graphStore.allNodes().toIterable
  }

  def allRelations(): Iterable[StoredRelation] = {
    graphStore.allRelations().toIterable
  }

  def createNodePropertyIndex(nodeLabel: String, propertyNames: Set[String]): Int = {
    val labelId = nodeLabelStore.id(nodeLabel)
    val propNameIds = propertyNameStore.ids(propertyNames)
    graphStore.createNodeIndex(labelId, propNameIds.toArray)
  }

  def writeNodeIndexRecord(indexId: Int, nodeId: Long, propertyValue: Any): Unit = {
    graphStore.insertNodeIndexRecord(indexId, nodeId, PropertyValueConverter.toBytes(propertyValue))
  }

  //FIXME: expensive time cost
  private def init(): Unit = {

  }

  def snapshot(): Unit = {
    //TODO: transaction safe
  }
}
