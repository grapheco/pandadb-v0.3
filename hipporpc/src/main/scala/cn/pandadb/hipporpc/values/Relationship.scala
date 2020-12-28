package cn.pandadb.hipporpc.values

case class Relationship(id: Long,
                        props: Map[String, AnyRef],
                        startNodeId: Long,
                        endNodeId: Long,
                        relationshipType: RelationshipType) extends Serializable {

  override def equals(o: Any): Boolean = {
    o.isInstanceOf[Relationship] && this.id == o.asInstanceOf[Relationship].id
  }
}
