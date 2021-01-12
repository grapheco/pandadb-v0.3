//package cn.pandadb.optimizer
//
//
//import org.junit.{Assert, Test}
//import org.opencypher.okapi.api.schema.PropertyGraphSchema
//import org.opencypher.okapi.api.table.CypherRecords
//import org.opencypher.okapi.api.value.CypherValue
//import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue, Node, Relationship}
//
//case class LynxNode(id: Long, labels: Set[String], props: (String, CypherValue)*) extends Node[Long] {
//  //lazy val properties = props.toMap
//  val withIds = props.toMap + ("_id" -> CypherValue(id))
//  override type I = this.type
//
//  override def copy(id: Long, labels: Set[String], properties: CypherMap): LynxNode.this.type = this
//
//  override def properties: CypherMap = props.toMap
//}
//
//case class LynxRelationship(id: Long, startId: Long, endId: Long, relType: String, props: (String, CypherValue)*) extends Relationship[Long] {
//
//  val withIds = props.toMap ++ Map("_id" -> CypherValue(id), "_from" -> CypherValue(startId), "_to" -> CypherValue(endId))
//  override type I = this.type
//
//  override def copy(id: Long, source: Long, target: Long, relType: String, properties: CypherMap): LynxRelationship.this.type = this
//
//  override def properties: CypherMap = props.toMap
//}
//
//
//class LinTest {
//
//
//
//  val _session = new PandaCypherSession()
//
//  val graphDemo = _session.createPropertyGraph(new PandaPropertyGraphScan[Long] {
//    val node1 = LynxNode(1, Set("person", "t1"), "name" -> CypherValue("bluejoe"), "age" -> CypherValue(40))
//    val node2 = LynxNode(2, Set("student", "t2"), "name" -> CypherValue("alex"), "age" -> CypherValue(30))
//    val node3 = LynxNode(3, Set("project", "t3"), "name" -> CypherValue("simba"), "age" -> CypherValue(10))
//
//    override def allNodes(): Iterable[Node[Long]] = Array(node1, node2, node3)
//
//    override def allRelationships(): Iterable[Relationship[Long]] = Array(
//      LynxRelationship(1, 1, 2, "knows"),
//      LynxRelationship(2, 2, 3, "knows")
//    )
//
//    override def schema: PropertyGraphSchema = PropertyGraphSchema.empty
//
//    /*
//      PropertyGraphSchemaImpl(
//        Map[Set[String], Map[String, CypherType]](
//          Set[String]() -> Map("name" -> CTString)
//        ),
//        Map[String, Map[String, CypherType]](
//          "knows" -> Map[String, CypherType]()
//        )
//      )
//     */
//    override def nodeAt(id: Long): Node[Long] = id match {
//      case 1L => node1
//      case 2L => node2
//      case 3L => node3
//    }
//  })
//
//
//  @Test
//  def testFileter1(): Unit = {
//    val rs = runOnDemoGraph("match (n) where n.name = 'bluejoe'  return n")
//    Assert.assertEquals(1, rs.collect.size)
//  }
//
//  @Test
//  def testFileter2(): Unit = {
//    val rs = runOnDemoGraph("match (n) where n.age = 40  return n")
//    Assert.assertEquals(1, rs.collect.size)
//  }
//
//  @Test
//  def testFileter3(): Unit = {
//    val rs = runOnDemoGraph("match (n:person) where n.age >30  return n")
//    Assert.assertEquals(1, rs.collect.size)
//  }
//
//  //@Test
//  def testFileter4(): Unit = {
//    val rs = runOnDemoGraph("match (n) where n.name STARTS WITH 'blue'  return n")
//  }
//
//  //@Test
//  def testFileter5(): Unit = {
//    val rs = runOnDemoGraph("match (n) where n.name ENDS WITH 'joe'  return n")
//  }
//
//  //@Test
//  def testFileter6(): Unit = {
//    val rs = runOnDemoGraph("match (n) where n.name =~ 'blue.*'  return n")
//  }
//
//  //@Test
//  def testFileter7(): Unit = {
//    val rs = runOnDemoGraph("match (n) where n.name =~ '(?i)blue.*'  return n")
//  }
//  @Test
//  def testFilter8(): Unit = {
//    val rs = runOnDemoGraph("match (n)  return n.name")
//    Assert.assertEquals(3, rs.collect.size)
//  }
//
//
//  private def runOnDemoGraph(query: String): CypherRecords = {
//    println(s"query: $query")
//    val t1 = System.currentTimeMillis()
//    val records = graphDemo.cypher(query)
//    val t2 = System.currentTimeMillis()
//    println(s"fetched records in ${t2 - t1} ms.")
//    records.show
//    records.records
//  }
//
//}
