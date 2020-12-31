package cn.pandadb.hipporpc.message

case class Messages(cypher: String){}
case class SayHelloRequest(msg: String){}
case class SayHelloResponse(msg: String){}
