package cn.pandadb.hipporpc.message

import cn.pandadb.VerifyConnectionMode

case class CypherRequest(cypher: String, params:Map[String, Any]){}

case class SayHelloRequest(msg: String){}
case class SayHelloResponse(msg: String){}

case class SecurityRequest(){}

case class VerifyConnectionRequest(username: String, password: String)
case class VerifyConnectionResponse(result: VerifyConnectionMode.Value)

case class ResetAccountRequest(username: String, password: String)
case class ResetAccountResponse(msg: VerifyConnectionMode.Value)
