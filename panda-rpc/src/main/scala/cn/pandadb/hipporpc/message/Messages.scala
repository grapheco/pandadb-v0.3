package cn.pandadb.hipporpc.message

import cn.pandadb.VerifyConnectionMode

case class CypherRequest(cypher: String, params:Map[String, Any]){}

case class TransactionCypherRequest(uuid: String, cypher: String, params:Map[String, Any]){}

case class TransactionCommitRequest(uuid: String){}
case class TransactionCommitResponse(msg: String){}

case class TransactionRollbackRequest(uuid: String){}
case class TransactionRollbackResponse(msg: String){}

case class SayHelloRequest(msg: String){}
case class SayHelloResponse(msg: String){}

case class SecurityRequest(){}

case class VerifyConnectionRequest(username: String, password: String)
case class VerifyConnectionResponse(result: VerifyConnectionMode.Value)

case class ResetAccountRequest(username: String, password: String)
case class ResetAccountResponse(msg: VerifyConnectionMode.Value)


case class GetStatisticsRequest()
case class GetStatisticsResponse(allNodes: Long, allRelations: Long, nodesCountByLabel: Map[String, Long], relsCountByType: Map[String, Long]){}