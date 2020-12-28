//package cn.pandadb.server.values
//
//case class Path(nodes: Array[Node],
//                relationships: Array[Relationship]) extends Serializable {
//
//  def nodeRepresentation(path: Path, node: Node): String = "(" + node.id + ")"
//
//  def relationshipRepresentation(path: Path, from: Node, relationship: Relationship): String = {
//    var prefix = "-"
//    var suffix = "-"
//    if (from == relationship.endNode) prefix = "<-"
//    else suffix = "->"
//    prefix + "[" + relationship.id + "]" + suffix
//  }
//
//  def toString[T <: Path](path: T) = {
//    var current = path.nodes(0)
//    val result = new StringBuilder
//    for (rel <- path.relationships) {
//      result.append(nodeRepresentation(path, current))
//      result.append(relationshipRepresentation(path, current, rel))
//      current = rel.endNode
//    }
//    if (null != current)
//      result.append(nodeRepresentation(path, current))
//    result.toString
//  }
//
//  override def equals(o: Any): Boolean = {
//    o.isInstanceOf[Path] && equalNode(this.nodes, o.asInstanceOf[Path].nodes) && equalRelationship(this.relationships, o.asInstanceOf[Path].relationships)
//  }
//
//  def equalNode(nodes1: Array[Node], nodes2: Array[Node]): Boolean = {
//    if (nodes1.length != nodes2.length)
//      false
//    else {
//      for (i <- 0 until nodes1.length) {
//        if (nodes1(i).id != nodes2(i).id)
//          false
//      }
//      true
//    }
//  }
//
//  def equalRelationship(Relationships1: Array[Relationship], Relationships2: Array[Relationship]): Boolean = {
//    if (Relationships1.length != Relationships2.length)
//      false
//    else {
//      for (i <- 0 until Relationships1.length) {
//        if (Relationships1(i).id != Relationships2(i).id)
//          false
//      }
//      true
//    }
//  }
//}
//
