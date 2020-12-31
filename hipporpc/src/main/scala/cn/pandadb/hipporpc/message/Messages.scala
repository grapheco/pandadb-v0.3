package cn.pandadb.hipporpc.message

import cn.pandadb.hipporpc.utils.DriverValue

case class CypherRequest(cypher: String){}
case class PeekOneDataRequest(cypher: String){}
case class PeekOneDataResponse(driverValue: DriverValue){}

case class SayHelloRequest(msg: String){}
case class SayHelloResponse(msg: String){}

case class VerifyConnectionRequest(username: String, password: String)
case class VerifyConnectionResponse(result: String)