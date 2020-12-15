package cn.pandadb.kv.performance

object querys {
  def getCypherCreateNode(id: Long, idStr: String, flag: Boolean, labels: Array[String] = Array()): String = {
    var labelStr = new StringBuilder()
    labels.foreach(labelStr += s":${_}")
    s"create(n${labelStr}:{id_p: ${id}, idStr: '${idStr}', flag: ${flag}})"
  }

  def getCypherCreateRel(fromIdStr: Long, toIdStr: Long, labels: Array[String] = Array()): String = {
    var labelStr = new StringBuilder()
    labels.foreach(labelStr += s":${_}")
    s"match (f), (t) where f.idStr='${fromIdStr}' AND t.idStr='${toIdStr}' CREATE (f)-[rel${labelStr}]->(s)"
  }

  def getCypherQueryByAttrEqual(equalString: String, limit: Int = 0): String = {
    val limitStr = limit match {
      case 0 => ""
      case _ => s" limit ${limit}"
    }
    s"match (n) where n.${equalString} return n${limitStr}"
  }

  def getCypherQueryByAttrGt(gtString: String, limit: Int = 0): String = {
    val limitStr = limit match {
      case 0 => ""
      case _ => s" limit ${limit}"
    }
    s"match (n) where n.${gtString} return n${limitStr}"
  }

  def getCypherQueryByRelFrom(fromId_p: Long, labels: Array[String] = Array()): String = {
    var labelStr = new StringBuilder()
    labels.foreach(labelStr += s":${_}")
    s"match (f)-[r${labelStr}]->(t) where f.id_p=${fromId_p} return Count(t)"
  }
}