package cn.pandadb.cypher.ldbc

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import org.junit.{After, Before, Test}

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
  def short1(): Unit ={
    val personId = 600000000000014L
    graphFacade.cypher(
      s"""
        |MATCH (n:person{id:'$personId'})-[:isLocatedIn]->(p:place)
        |RETURN
        |n,
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
  def tmp(): Unit ={
    val personId = 600000000000014L
    graphFacade.cypher(s"MATCH (:person{id:'$personId'})<-[:hasCreator]-(m:comment)-[:replyOf]->(p:post) return exists(m.content)").show()
  }
  @Test
  def short2(): Unit ={
    //change 【replyOf*0..】-->【replyOf】
    // TODO: Join slow down
    val personId = 600000000000014L
    graphFacade.cypher(
      s"""
        |MATCH (:person{id:'$personId'})<-[:hasCreator]-(m:comment)-[:replyOf]->(p:post)
        |MATCH (p)-[:hasCreator]->(c)
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
    val personId = 600000000000014L
        graphFacade.cypher(
      s"""
        |MATCH (n:person {id:'$personId'})-[r:knows]-(friend)
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
    val commentId = 700000000032435L
    graphFacade.cypher(
      s"""
        |MATCH (m:comment {id:'$commentId'})
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
    val commentId = 700000000032435L
    graphFacade.cypher(
      s"""
        |MATCH (m:comment {id:'$commentId'})-[:hasCreator]->(p:person)
        |RETURN
        |  p.id AS personId,
        |  p.firstName AS firstName,
        |  p.lastName AS lastName
        |""".stripMargin).show()
  }
  @Test
  def short6(): Unit ={
    //change 【replyOf*0..】-->【replyOf】

    val commentId = 700000000032435L
    graphFacade.cypher(
      s"""
        |MATCH (m:comment {id:'$commentId'})-[:replyOf]->(p:post)<-[:containerOf]-(f:forum)-[:hasModerator]->(mod:person)
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
    // TODO: optimize speed
    val commentId = 700000000032435L
    graphFacade.cypher(
      s"""
        |MATCH (m:comment {id:'$commentId'})<-[:replyOf]-(c:comment)-[:hasCreator]->(p:person)
        |OPTIONAL MATCH (m)-[:hasCreator]->(a:person)-[r:knows]-(p)
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
  @Test
  def t1_ordinary_property(): Unit ={
    //ok
    val firstName = "Wilhelm"
    graphFacade.cypher(s"MATCH(n:person{firstName: '$firstName'}) RETURN count(n)").show()
  }
  @Test
  def t2_unique_property(): Unit ={
    // ok
    val commentId = 700755914244433L
    graphFacade.cypher(
      s"""
         |MATCH (m:comment {id:'$commentId'})
         |RETURN
         |  m.creationDate AS messageCreationDate,
         |  m.content as content
         |""".stripMargin).show()
  }
  @Test
  def t3_degree1_getId(): Unit ={
    val personId = 606597069766772L
    graphFacade.cypher(
      s"""
         |MATCH (n:person {id:'$personId'})-[r:knows]-(friend)
         |RETURN count(id(friend))
         |""".stripMargin).show()
  }
  @Test
  def t4_degree1_getNodeData(): Unit ={
    // total number ok
    val personId = 600000000001008L
    val res = graphFacade.cypher(
      s"""
         |MATCH (n:person {id:'$personId'})-[r:knows]-(friend)
         |RETURN
         |  friend.id AS personId,
         |  friend.firstName AS firstName,
         |  friend.lastName AS lastName,
         |  r.creationDate AS friendshipCreationDate
         |""".stripMargin).show()
  }
  @Test
  def t5_degree1_getNodeData(): Unit ={
    // ok
    val personId = 600000000001008L
    graphFacade.cypher(
      s"""
         |MATCH (n:person {id:'$personId'})-[:isLocatedIn]->(p:place)
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
  def t6_degree1_getNodeData(): Unit ={
    // ok, message --> comment
    val commentId = 700274877907296L
    graphFacade.cypher(
      s"""
         |MATCH (m:comment {id:'$commentId'})-[:hasCreator]->(p:person)
         |RETURN
         |  p.id AS personId,
         |  p.firstName AS firstName,
         |  p.lastName AS lastName
         |""".stripMargin).show()
  }
  @Test
  def t7_degree2_get_id(): Unit ={
    val personId = 600000000001008L
    graphFacade.cypher(
      s"""
         |MATCH (n:person {id:'$personId'}) -[:knows]-> () -[:knows]-> (m:person)
         |RETURN id(m)
         |""".stripMargin).show()
  }
  @Test
  def t8_degree2_get_property(): Unit ={
    // total num ok
    val personId = 600000000001008L
    graphFacade.cypher(
      s"""
         |MATCH (n:person {id:'$personId'}) -[:knows]-> () -[:knows]-> (m:person)
         |RETURN m.firstName AS firstName,
         |       m.lastName AS lastName,
         |       m.birthday AS birthday,
         |       m.locationIP AS locationIP,
         |       m.browserUsed AS browserUsed
         |""".stripMargin).show()
  }
  @Test
  def t9_degree3(): Unit ={
    // total num ok
    val personId = 600000000001008L
    val res = graphFacade.cypher(
      s"""
         |MATCH (:person {id:'$personId'})<-[:hasCreator]-(m)-[:replyOf]->(p:post)-[:hasCreator]->(c)
         |RETURN
         |  m.id AS messageId,
         |  m.creationDate AS messageCreationDate,
         |  p.id AS originalPostId,
         |  c.id AS originalPostAuthorId,
         |  c.firstName AS originalPostAuthorFirstName,
         |  c.lastName AS originalPostAuthorLastName
         |""".stripMargin).show()
  }
  @Test
  def t10_degree3(): Unit ={
    // ok
    val commentId = 700755914244428L
    graphFacade.cypher(
      s"""
         |MATCH (m:comment{id:'$commentId'})-[:replyOf]->(p:post)<-[:containerOf]-(f:forum)-[:hasModerator]->(mod:person)
         |RETURN
         |  f.id AS forumId,
         |  f.title AS forumTitle,
         |  mod.id AS moderatorId,
         |  mod.firstName AS moderatorFirstName,
         |  mod.lastName AS moderatorLastName
         |""".stripMargin).show()
  }
  @Test
  def t11_degree2(): Unit ={
    // ok
    val postId = 800755914244427L
    graphFacade.cypher(
      s"""
         |MATCH (m:post{id:'$postId'})<-[:replyOf]-(c:comment)-[:hasCreator]->(p:person)
         |RETURN
         |  c.id AS commentId,
         |  c.content AS commentContent,
         |  c.creationDate AS commentCreationDate,
         |  p.id AS replyAuthorId,
         |  p.firstName AS replyAuthorFirstName,
         |  p.lastName AS replyAuthorLastName
         |""".stripMargin).show()
  }
  @Test
  def t12_degree2(): Unit ={
    // ok
    val postId = 800755914244427L
    graphFacade.cypher(
      s"""
         |MATCH (m:post{id:'$postId'})-[:hasCreator]->(a:person)-[r:knows]-(p)
         |RETURN
         |  m.id AS postId,
         |  m.language as postLanguage,
         |  p.id AS replyAuthorId,
         |  p.firstName AS replyAuthorFirstName,
         |  p.lastName AS replyAuthorLastName
         |""".stripMargin).show()
  }
  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
