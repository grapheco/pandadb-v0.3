package org.grapheco.pandadb.net.rpc.message


case class CypherRequest(cypher: String, params:Map[String, Any]){}

case class TransactionCypherRequest(uuid: String, cypher: String, params:Map[String, Any]){}

case class TransactionCommitRequest(uuid: String){}
case class TransactionCommitResponse(msg: String){}

case class TransactionRollbackRequest(uuid: String){}
case class TransactionRollbackResponse(msg: String){}

case class SayHelloRequest(msg: String){}
case class SayHelloResponse(msg: String){}

case class GetStatisticsRequest()
case class GetStatisticsResponse(allNodes: Long, allRelations: Long, nodesCountByLabel: Map[String, Long], relationsCountByType: Map[String, Long], propertiesCountByIndex: Map[String, Long]){}

case class GetIndexedMetaRequest()
case class GetIndexedMetaResponse(metaMap: Map[String, Seq[String]])

case class CreateIndexRequest(label: String, propNames: Seq[String])
case class CreateIndexResponse(accept: Boolean)

case class DropIndexRequest(label: String, propName: String)
case class DropIndexResponse(accept: Boolean)