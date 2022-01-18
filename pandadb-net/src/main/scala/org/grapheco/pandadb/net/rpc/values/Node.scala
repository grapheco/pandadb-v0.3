package org.grapheco.pandadb.net.rpc.values

case class Node(id: Long,
                props: Map[String, Value],
                labels: Array[Label]) extends Serializable {

  override def toString: String = {
    def printLabel(): String = {
      var labelString: String = ""
      for (l <- labels) labelString += s":$l"
      labelString
    }

    s"Node($id,$props,($printLabel))"
  }

  override def equals(o: Any): Boolean = {
    o.isInstanceOf[Node] && this.id == o.asInstanceOf[Node].id
  }
}
