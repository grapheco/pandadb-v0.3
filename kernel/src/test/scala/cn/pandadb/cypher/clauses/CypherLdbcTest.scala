package cn.pandadb.cypher.clauses

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import org.junit.{Before, Test}

class CypherLdbcTest {
  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacade = _

  @Before
  def init(): Unit ={
    val dbPath = "D:\\data\\ldbc-0.1.panda.db"

    nodeStore = new NodeStoreAPI(dbPath)
    relationStore = new RelationStoreAPI(dbPath)
    indexStore = new IndexStoreAPI(dbPath)
    statistics = new Statistics(dbPath)

    graphFacade = new GraphFacade(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )
  }

  @Test
  def tmp(): Unit ={
    graphFacade.cypher("match (n:message) return n limit 5").show()

  }

  @Test
  def short1(): Unit ={
    val personId = 600000000000016L
    graphFacade.cypher(
      s"""
        |MATCH (n:person {id:$personId})-[:IS_LOCATED_IN]->(p:place)
        |RETURN
        |  n.firstName AS firstName,
        |  n.lastName AS lastName,
        |  n.birthday AS birthday,
        |  n.locationIP AS locationIP,
        |  n.browserUsed AS browserUsed,
        |  p.id AS cityId,
        |  n.gender AS gender,
        |  n.creationDate AS creationDate
        |""".stripMargin).show()
  }
  @Test
  def short2(): Unit ={
    val personId = 600000000000016L

    graphFacade.cypher(
      s"""
        |MATCH (:Person {id:$personId})<-[:HAS_CREATOR]-(m:Message)-[:REPLY_OF*0..]->(p:Post)
        |MATCH (p)-[:HAS_CREATOR]->(c)
        |RETURN
        |  m.id AS messageId,
        |  CASE exists(m.content)
        |    WHEN true THEN m.content
        |    ELSE m.imageFile
        |  END AS messageContent,
        |  m.creationDate AS messageCreationDate,
        |  p.id AS originalPostId,
        |  c.id AS originalPostAuthorId,
        |  c.firstName AS originalPostAuthorFirstName,
        |  c.lastName AS originalPostAuthorLastName
        |ORDER BY messageCreationDate DESC
        |LIMIT 10
        |""".stripMargin).show()
  }
  @Test
  def short3(): Unit ={
    val personId = 600000000000016L

    graphFacade.cypher(
      s"""
        |MATCH (n:Person {id:$personId})-[r:KNOWS]-(friend)
        |RETURN
        |  friend.id AS personId,
        |  friend.firstName AS firstName,
        |  friend.lastName AS lastName,
        |  r.creationDate AS friendshipCreationDate
        |ORDER BY friendshipCreationDate DESC, toInteger(personId) ASC
        |""".stripMargin).show()
  }
  @Test
  def short4(): Unit ={
    val messageId = 600000000000016L
    graphFacade.cypher(
      s"""
        |MATCH (m:Message {id:$messageId})
        |RETURN
        |  m.creationDate AS messageCreationDate,
        |  CASE exists(m.content)
        |    WHEN true THEN m.content
        |    ELSE m.imageFile
        |  END AS messageContent
        |""".stripMargin).show()
  }
  @Test
  def short5(): Unit ={
    val messageId = 600000000000016L

    graphFacade.cypher(
      s"""
        |MATCH (m:Message {id:$messageId})-[:HAS_CREATOR]->(p:Person)
        |RETURN
        |  p.id AS personId,
        |  p.firstName AS firstName,
        |  p.lastName AS lastName
        |""".stripMargin).show()
  }
  @Test
  def short6(): Unit ={
    val messageId = 600000000000016L

    graphFacade.cypher(
      s"""
        |MATCH (m:Message {id:$messageId})-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)
        |RETURN
        |  f.id AS forumId,
        |  f.title AS forumTitle,
        |  mod.id AS moderatorId,
        |  mod.firstName AS moderatorFirstName,
        |  mod.lastName AS moderatorLastName
        |""".stripMargin).show()
  }
  @Test
  def short7(): Unit ={
    val messageId = 600000000000016L

    graphFacade.cypher(
      s"""
        |MATCH (m:Message {id:$messageId})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
        |OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
        |RETURN
        |  c.id AS commentId,
        |  c.content AS commentContent,
        |  c.creationDate AS commentCreationDate,
        |  p.id AS replyAuthorId,
        |  p.firstName AS replyAuthorFirstName,
        |  p.lastName AS replyAuthorLastName,
        |  CASE r
        |    WHEN null THEN false
        |    ELSE true
        |  END AS replyAuthorKnowsOriginalMessageAuthor
        |ORDER BY commentCreationDate DESC, replyAuthorId
        |""".stripMargin).show()
  }
}
