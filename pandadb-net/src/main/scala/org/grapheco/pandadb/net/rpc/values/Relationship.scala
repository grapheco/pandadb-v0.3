package org.grapheco.pandadb.net.rpc.values

case class Relationship(id: Long,
                        props: Map[String, Value],
                        startNodeId: Long,
                        endNodeId: Long,
                        relationshipType: RelationshipType) extends Serializable {

  override def equals(o: Any): Boolean = {
    o.isInstanceOf[Relationship] && this.id == o.asInstanceOf[Relationship].id
  }
}
