package cn.pandadb.kernel

import org.opencypher.okapi.api.graph.CypherResult

trait GraphService {
  type Id = Long

  def cypher(query: String, parameters: Map[String, Any] = Map.empty): CypherResult

  def close(): Unit

  def addNode(nodeProps: Map[String, Any], labels: String*): this.type

  def addRelation(label: String, from: Long, to: Long, relProps: Map[String, Any]): this.type

  def deleteNode(id: Id): this.type

  def deleteRelation(id: Id): this.type
}
