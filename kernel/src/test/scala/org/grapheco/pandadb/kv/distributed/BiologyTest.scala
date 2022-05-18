package org.grapheco.pandadb.kv.distributed

import org.apache.http.HttpHost
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder
import org.grapheco.lynx.{LynxNodeLabel, LynxPropertyKey, LynxValue, NodeFilter}
import org.grapheco.pandadb.kernel.distribute.relationship.RelationDirection
import org.grapheco.pandadb.kernel.distribute.{DistributedGraphFacade, DistributedKeyConverter}
import org.grapheco.pandadb.kernel.kv.ByteUtils
import org.grapheco.pandadb.kernel.store.{PandaNode, PandaRelationship, StoredRelation}
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import org.junit.{After, Before, Test}
import org.tikv.common.util.ScanOption
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.raw.RawKVClient
import org.tikv.shade.com.google.protobuf.ByteString

import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConverters, breakOut, mutable}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-03-28 18:36
 */
class BiologyTest {
  var api: DistributedGraphFacade = _
  val kvHosts = "10.0.82.144:2379,10.0.82.145:2379,10.0.82.146:2379"
  val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
  val udpClient = Array(new UDPClient("127.0.0.1", 6000))

  var tikv: RawKVClient = _

  @Before
  def init(): Unit = {
    val conf = TiConfiguration.createRawDefault(kvHosts)
    val session = TiSession.create(conf)
    tikv = session.createRawClient()
    api = new DistributedGraphFacade(kvHosts, indexHosts, new UDPClientManager(udpClient))
  }

  @Test
  def unknown1(): Unit ={
    // MATCH (n:taxonomy) where n.tax_id ='9606' RETURN n as node
    val node = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
    println(node)
  }
  @Test
  def unknown2(): Unit ={
    // MATCH (n:taxonomy {tax_id:'9605'}) RETURN n.scientific_name as scientific_name
    val node = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9605")))).next()
    println(node.property(LynxPropertyKey("scientific_name")))
  }
  @Test
  def unknown3(): Unit = {
    // MATCH (t:taxonomy{tax_id:'9606'})-[:produce]->(p:pubmed) where 2018 > toInteger(left(p.publish_date, 4)) RETURN count(p) as num
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
    val relType = api.getRelationTypeId("produce").get
    val labelId = api.getNodeLabelId("pubmed").get
    val endNodeIds = api.findOutRelationsEndNodeIds(startNode.id.value, relType)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, labelId))
    val count = resNodes.count(n => n.props(LynxPropertyKey("publish_date")).value.toString.slice(0, 4).toInt < 2018)
    println(count)
  }
  @Test
  def unknown4(): Unit ={
    //MATCH (t:taxonomy {tax_id:'9606'})-[r:produce]->(p:pubmed)
    // where p.keywords is not null and toInteger(left(p.publish_date,4)) >= 2012
    // unwind(split(toLower(p.keywords),';')) as keyword with distinct keyword as k,
    // count(keyword) as num order by num desc limit 20
    // match (t:taxonomy {tax_id:'9606'})-[:produce]->(p:pubmed)
    // where p.keywords is not null and toInteger(left(p.publish_date,4)) >= 2012 and k in split(toLower(p.keywords),';')
    // return distinct left(p.publish_date,4) as year, k, count(p) as num order by year asc;
    topKTendencyAPI()
  }
  @Test
  def unknownF5(): Unit ={
    // MATCH (t:taxonomy{tax_id:'9606'})-[:produce]->(p:pubmed) RETURN sum(toInteger(p.cited_num)) as num
    var start = System.nanoTime()
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
    val relType = api.getRelationTypeId("produce").get
    val pubmedId = api.getNodeLabelId("pubmed").get
    val endNodeIds = api.findOutRelationsEndNodeIds(startNode.id.value, relType)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, pubmedId))
    start = System.nanoTime()
    // big cost
    val count = resNodes
      .map(n => {
        val res = n.props(LynxPropertyKey("cited_num")).value.toString
        if (res.nonEmpty) res.toInt
        else 0
      }).sum
    println(count)
  }
  @Test
  def unknownF7(): Unit ={
    //MATCH (t:taxonomy{tax_id:'9606'})-[:produce]->(p:pubmed) where 2018 > toInteger(left(p.publish_date, 4)) RETURN sum(toInteger(p.cited_num)) as num
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
    val relType = api.getRelationTypeId("produce").get
    val pubmedId = api.getNodeLabelId("pubmed").get
    val endNodeIds = api.findOutRelationsEndNodeIds(startNode.id.value, relType)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, pubmedId))
      .filter(p => p.props(LynxPropertyKey("publish_date")).value.toString.slice(0, 4).toInt < 2018)
    // big cost
    val count = resNodes
      .map(n => {
        val res = n.props(LynxPropertyKey("cited_num")).value.toString
        if (res.nonEmpty) res.toInt
        else 0
      }).sum
    println(count)
  }

  @After
  def close(): Unit = {
    api.close()
  }

  @Test
  def createIndex(): Unit = {
    api.createIndexOnNode("taxonomy", Set("tax_id"))
  }

  def timeCost(f: () => Unit): Unit = {
    val start = System.nanoTime()
    f()
    val end = System.nanoTime() - start
    println(s"total time cost: ${TimeUnit.NANOSECONDS.toMillis(end)} ms")
  }


  @Test
  def basicInfoOfTaxonomy(): Unit = {
    // 214ms
    api.cypher("""MATCH (n: taxonomy) where n.tax_id ='9606' RETURN n""")
  }

  /**
   * 物种基本信息
   */
  @Test
  def basicInfoOfTaxonomyAPI(): Unit = {
    // match\s*\(.*\)\s*where\s*\S*\s*=\s*'\S*' return\s*n
    timeCost(() => {
      val node = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      println(node)
    })
  }

  @Test
  def totalPubmedOfTaxonomy(): Unit = {
    // 266ms
    val res = api.cypher("""MATCH (t:taxonomy{tax_id:'9606'})-[:produce]->(p:pubmed) RETURN count(p) as num""").show()
  }

  /**
   * 查询物种总文献量
   */
  @Test
  def totalPubmedOfTaxonomyAPI(): Unit = {
    // 137ms
    // match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return count\S* as \S*
    timeCost(() => {
      val node = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("produce").get
      val res = api.countOutRelations(node.id.value, relType)
      println(res)
    })
  }


  @Test
  def countCited(): Unit = {
    //327ms
    api.cypher("""MATCH (t:taxonomy{tax_id:'9606'})-[:produce]->(p:pubmed) RETURN sum(toInteger(p.cited_num)) as num""")
  }

  /**
   * 查询物种总被引文献量
   */
  @Test
  def countCitedAPI(): Unit = {
    // 1636 ms
    // match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return sum\s*\(\s*tointeger.*\) as \S*
    timeCost(() => {
      var start = System.nanoTime()
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("produce").get
      val pubmedId = api.getNodeLabelId("pubmed").get
      val endNodeIds = api.findOutRelationsEndNodeIds(startNode.id.value, relType)
      val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, pubmedId))
      println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
      start = System.nanoTime()
      // big cost
      val count = resNodes
        .map(n => {
          val res = n.props(LynxPropertyKey("cited_num")).value.toString
          if (res.nonEmpty) res.toInt
          else 0
        }).sum
      println(count)
      println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
    })
  }


  @Test
  def count3YearCited(): Unit = {
    // 286ms
    api.cypher("""MATCH (t:taxonomy{tax_id:'9606'})-[:produce]->(p:pubmed) where 2019 < toInteger(left(p.publish_date, 4)) RETURN count(p) as num""")
  }

  /**
   * 近三年总文献量
   */
  @Test
  def count3YearCitedAPI(): Unit = {
    // 1546 ms
    //match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*where \S*\s*<\s*tointeger.*\) return count\S* as \S*
    timeCost(() => {
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("produce").get
      val labelId = api.getNodeLabelId("pubmed").get
      val endNodeIds = api.findOutRelationsEndNodeIds(startNode.id.value, relType)
      val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, labelId))

      val start = System.nanoTime()
      val count = resNodes.count(n => n.props(LynxPropertyKey("publish_date")).value.toString.slice(0, 4).toInt > 2019)

      println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
      println(count)
    })
  }


  @Test
  def earliest(): Unit = {
    // 274ms
    api.cypher("""MATCH (t:taxonomy{tax_id:'9606'})-[:produce]->(p:pubmed) RETURN min(left(p.publish_date, 4)) as year""")
  }

  /**
   * 最早研究年度
   */
  @Test
  def earliestAPI(): Unit = {
    // 1681ms
    // match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return min.*\) as \S*
    timeCost(() => {
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("produce").get
      val labelId = api.getNodeLabelId("pubmed").get
      val endNodeIds = api.findOutRelationsEndNodeIds(startNode.id.value, relType)
      val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, labelId))
      val start = System.nanoTime()
      val res = resNodes.map(n => n.props(LynxPropertyKey("publish_date")).value.toString.slice(0, 4).toInt).min
      println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
      println(res)
    })
  }

  @Test
  def findTop3LevelParent(): Unit = {
    // 374ms
    api.cypher("""match t=(t0:taxonomy{tax_id:'9606'})-[r:parent*1..3]->(t2:taxonomy) return t""".stripMargin)
  }

  /**
   * 查询物种的三级父信息
   */
  @Test
  def findTop3LevelParentAPI(): Unit = {
    // 852ms
    // match \S*=.*\[.*1..3\].* return \S*
    // ()-[]->()
    // ()-[]->()-[]->()
    // () -[]->() -[]->() -[]->()
    timeCost(() => {
      var result: Seq[Seq[LynxValue]] = Seq.empty
      val typeId = api.getRelationTypeId("parent")
      val node1 = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val path1 = outPattern(Seq(node1.id.value), typeId, "taxonomy")
      val path2 = outPattern(path1.map(f => f._2.id.value), typeId, "taxonomy")
      val path3 = outPattern(path2.map(f => f._2.id.value), typeId, "taxonomy")

      val data1 = path1.flatMap(f => Seq(node1, f._1, f._2))
      val data2 = path2.flatMap(f => data1 ++ Seq(f._1, f._2))
      val data3 = path3.flatMap(f => data2 ++ Seq(f._1, f._2))
      result = Seq(data1, data2, data3)
      println(result)
    })
  }

  def outPattern(startNodeIds: Seq[Long], relTypeId: Option[Int], endNodesLabel: String): Seq[(PandaRelationship, PandaNode)] = {
    val pattern = startNodeIds.flatMap(leftId => {
      val r = api.findOutRelations(leftId, relTypeId)
      r.map(rr => (rr, api.getNodeById(rr.endNodeId.value, endNodesLabel).get))
    })
    pattern
  }


  @Test
  def projectInfo(): Unit = {
    // bigCost: 2838ms
    api.cypher(
      """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy_bioproject]->(b:bioproject)
        |return b.title as title, b.bioproject_id as bioproject_id order by b.smdt desc limit 20""".stripMargin)
  }

  /**
   * 查询物种项目信息
   */
  @Test
  // match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return \S*.title.*bioproject_id.*smdt.*limit \S*
  // 9838 ms
  def projectInfoAPI(): Unit = {
    val pattern = Pattern.compile(".*/.*")
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
    val endNodeIds = api.findOutRelationsEndNodeIds(startNode.id.value, api.getRelationTypeId("taxonomy_bioproject").get)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, api.getNodeLabelId("bioproject").get))

    val queue = new mutable.PriorityQueue[(String, (String, String))]()((a,b)=> -a._1.compare(b._1) ) // 小根堆，每次pop最小
    resNodes.foreach(node => {
      val smdt = node.props(LynxPropertyKey("smdt")).value.toString
      if (pattern.matcher(smdt).matches()){
        val title = node.props(LynxPropertyKey("title")).value.toString
        val bioproject_id = node.props(LynxPropertyKey("bioproject_id")).value.toString
        queue.enqueue((smdt,(title, bioproject_id)))
        if (queue.size > 10) queue.dequeue()
      }
    })

    val r = queue.toList.sortBy(f => f._1).reverse.map(f => Seq(f._2._1, f._2._2))
    r.foreach(println)
  }

  @Test
  def geneOfTaxonomy(): Unit = {
    // 221ms
    api.cypher("""MATCH (t:taxonomy{tax_id:'9606'})-[r:taxonomy_gene]->(g:gene) RETURN g.title as title, g.gene_id as gene_id limit 20""")
  }

  /**
   * 查询物种基因信息
   */
  @Test
  def geneOfTaxonomyAPI(): Unit = {
    // match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return.*title.*gene_id limit \S*
    // 471ms
    timeCost(() => {
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("taxonomy_gene")
      val endNodeIds = api.findOutRelations(startNode.id.value, relType).map(f => f.endNodeId.value)
      val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, "gene"))
      val res = resNodes.slice(0, 20).map(f => (f.props(LynxPropertyKey("title")).value, f.props(LynxPropertyKey("gene_id")).value))
      res.foreach(println)
    })
  }


  @Test
  def genomeOfTaxonomy(): Unit = {
    // 118ms
    api.cypher("""MATCH (t:taxonomy{tax_id:'9606'})-[r:taxonomy_genome]->(g:genome) RETURN g.genome_id as genome_id, g.aacc as aacc limit 20""")
  }

  /**
   * 查询物种基因组信息
   */
  @Test
  def genomeOfTaxonomyAPI(): Unit = {
    // match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return.*genome_id.*aacc limit \S*
    // 365ms
    timeCost(() => {
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("taxonomy_genome")
      val endNodeIds = api.findOutRelations(startNode.id.value, relType).map(f => f.endNodeId.value)
      val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, "genome"))
      val res = resNodes.slice(0, 20).map(f => (f.props(LynxPropertyKey("genome_id")).value, f.props(LynxPropertyKey("aacc")).value))
      res.foreach(println)
    })
  }


  @Test
  def paperTendency(): Unit = {
    // 285ms
    api.cypher(
      """MATCH (t:taxonomy {tax_id:'9606'})-[r:produce]->(p:pubmed) where toInteger(left(p.publish_date, 4)) >= 1980
        |RETURN distinct left(p.publish_date, 4) as year , count(p) as num order by year asc""".stripMargin)
  }

  /**
   * 物种论文发表趋势
   */
  @Test
  def paperTendencyAPI(): Unit = {
    // match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where tointeger.*>=\s*\S* return .*publish_date.*count\S* as \S* order by \S* \S*
    // 1962 ms
    timeCost(() => {
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("produce")
      val endNodeIds = api.findOutRelations(startNode.id.value, relType).map(f => f.endNodeId.value)
      val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, "pubmed"))
      val yearCountMap = mutable.Map[Int, Int]()
      var start = System.nanoTime()
      resNodes.foreach(node => {
        val year = node.props(LynxPropertyKey("publish_date")).value.toString.slice(0, 4).toInt
        if (year >= 1980) {
          if (yearCountMap.contains(year)) {
            yearCountMap(year) = yearCountMap(year) + 1
          }
          else yearCountMap += year -> 1
        }
      })
      println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
      start = System.nanoTime()
      yearCountMap.toSeq.sortBy(f => f._1).foreach(println)
      println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
    })
  }

  @Test
  def topKTendency(): Unit = {
    // 426ms
    api.cypher(
      """MATCH (t:taxonomy {tax_id:'9606'})-[r:produce]->(p:pubmed)
        |where p.keywords is not null and
        |toInteger(left(p.publish_date,4)) >= 1970  and toInteger(left(p.publish_date,4)) <= 2021
        |unwind(split(toLower(p.keywords),';')) as keyword with distinct keyword as k,
        |count(keyword) as num order by num desc limit 20
        |match (t:taxonomy {tax_id:'9606'})-[:produce]->(p:pubmed)
        |where p.keywords is not null and toInteger(left(p.publish_date,4)) >= 1970 and toInteger(left(p.publish_date,4)) <= 2021 and
        |k in split(toLower(p.keywords),';')
        |return distinct left(p.publish_date,4) as year, k, count(p) as num order by year asc""".stripMargin)
  }

  /**
   * 物种TOP-N热点词发展趋势
   */
  @Test
  def topKTendencyAPI(): Unit = {
    //match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where.*>= \S*.*<= \S* unwind.*>=.*<=.* return.*publish_date.*count\S* as \S* order by \S* \S*
    //1814 ms
    timeCost(() => {
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("produce")
      val endNodeIds = api.findOutRelations(startNode.id.value, relType).map(f => f.endNodeId.value)
      val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, "pubmed"))

      val betweenYearData = resNodes.filter(p => {
        p.props(LynxPropertyKey("keywords")).value.toString.nonEmpty && {
          val year = p.props(LynxPropertyKey("publish_date")).value.toString.slice(0, 4).toInt
          year >= 1970 && year <= 2021
        }
      }).duplicate

      val start = System.nanoTime()
      val keywords = betweenYearData._1.flatMap(p => p.props(LynxPropertyKey("keywords")).value.toString.split(";").distinct)
        .toSeq
        .groupBy(k => k)
        .map(f => (f._1, f._2.length)).toSeq
        .sortBy(f => f._2).reverse.slice(0, 20).map(f => (f._1.toLowerCase, f._2)).toMap
      println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))

      val resMap = mutable.Map[(Int, String), Int]()
      betweenYearData._2.foreach(node => {
        val keywds = node.props(LynxPropertyKey("keywords")).value.toString.toLowerCase().split(";")
        val year = node.props(LynxPropertyKey("publish_date")).value.toString.slice(0, 4).toInt
        keywds.foreach(key => {
          if (keywords.contains(key)) {
            if (resMap.contains((year, key))) resMap((year, key)) = resMap((year, key)) + 1
            else resMap += (year, key) -> 1
          }
        })
      })

      val res = resMap.toSeq.sortBy(f => f._1._1)
      res.foreach(f => println(s"year:${f._1._1}, keyword:${f._1._2}, count: ${f._2}"))
    })
  }

  @Test
  def keywordRelation(): Unit = {
    // 621ms
    api.cypher(
      """MATCH (t:taxonomy {tax_id:'9606'})-[r:produce]->(p:pubmed)
        |where p.keywords is not null with (split(toLower(p.keywords),';')) as keywords
        |with keywords as keywords1, keywords as keywords2
        |unwind keywords1 as keyword1 unwind keywords2 as keyword2
        |with (keyword1 + ';' + keyword2) as keyword where keyword1 <> keyword2
        |return distinct keyword as keyword, count(keyword) as num order by num desc limit 10""".stripMargin)
  }

  /**
   * 关键词关系
   */
  @Test
  def keywordRelationAPI(): Unit = {
    // match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where.*with \S* as \S*.*unwind.*<>.*limit \S*
    // 3444ms
    timeCost(() => {
      val key = LynxPropertyKey("keywords")
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("produce")
      val endNodeIds = api.findOutRelations(startNode.id.value, relType).map(f => f.endNodeId.value)
      val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, "pubmed"))
      val keywords = resNodes.filter(p => p.props(key).value.toString.nonEmpty).map(p => p.props(key).value.toString.trim.toLowerCase().split(";")).toSeq

      val leftRight = keywords.map(line => {
        val length = line.length
        val left = ArrayBuffer[String]()
        val right = ArrayBuffer[String]()
        var outCount = 0
        while (outCount < length) {
          val target = line(outCount)
          var innerCount = 0
          while (innerCount < length) {
            left.append(target)
            right.append(line(innerCount))
            innerCount += 1
          }
          outCount += 1
        }
        (left, right)
      })

      val res = leftRight.flatMap(data => {
        val length = data._1.length
        val _res: ArrayBuffer[String] = ArrayBuffer.empty
        var count = 0
        while (count < length) {
          val left = data._1(count)
          val right = data._2(count)
          if (left != right) {
            _res.append(s"$left;$right")
          }
          count += 1
        }
        _res
      })
      val countMap: mutable.Map[String, Int] = mutable.Map.empty
      val queue = new mutable.PriorityQueue[(String, Int)]()(Ordering.by(a => -a._2)) // 小根堆，每次pop最小
      res.foreach(k => {
        if (countMap.contains(k)) countMap(k) = countMap(k) + 1
        else countMap += k -> 1
      })
      countMap.foreach(kv => {
        queue.enqueue(kv)
        if (queue.length > 10) queue.dequeue()
      })
      queue.toArray.sortBy(f => f._2).reverse.foreach(println)
    })
  }

  @Test
  def countKey(): Unit = {
    // 305ms
    api.cypher(
      """MATCH (t:taxonomy {tax_id:'9606'})-[r:produce]->(p:pubmed)
        |where p.keywords is not null with (split(toLower(p.keywords),';')) as keywords
        |unwind keywords as keyword
        |return distinct keyword, count(keyword) as num order by num desc limit 10""".stripMargin)
  }

  /**
   * 关键词数量
   */
  @Test
  def countKeyAPI(): Unit = {
    // match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where.* with.*unwind \S* as \S* return .*count.*limit \S*
    // 1664ms
    timeCost(() => {
      val key = LynxPropertyKey("keywords")
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("produce")
      val endNodeIds = api.findOutRelations(startNode.id.value, relType).map(f => f.endNodeId.value)
      val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, "pubmed"))

      val keywordsArr = ArrayBuffer[String]()

      resNodes.foreach(node => {
        if (node.props(key).value.toString.nonEmpty) {
          val line = node.props(key).value.toString.trim.toLowerCase.split(";")
          keywordsArr.append(line: _*)
        }
      })

      val a = keywordsArr.groupBy(f => f).map(f => (f._1, f._2.length)).toSeq.sortBy(f => f._2).reverse.slice(0, 10)
      a.foreach(println)
    })
  }

  @Test
  def distributionOfCountryOfPaper(): Unit = {
    // 754ms
    api.cypher(
      """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed)-[:pubmed2country]->(c:map_country)
        |RETURN c.country as country, count(c.country) as num""".stripMargin)
  }

  /**
   * 物种研究国家分布情况(论文)
   */
  @Test
  def distributionOfCountryOfPaperAPI(): Unit = {
    // match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed.*\)\s*-\[.*\]\s*->\s*\(.*map_country.*\) return.*country.*count\(.* as \S*
    // 5758 ms
    timeCost(() => {
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("taxonomy_pubmed")
      val relType2 = api.getRelationTypeId("pubmed_country")
      val labelType = api.getNodeLabelId("map_country").get
      val endNodeIds = api.findOutRelations(startNode.id.value, relType).map(f => f.endNodeId.value).toSeq.sorted
      val opts = endNodeIds.map(id => {
        val start = DistributedKeyConverter.edgeKeyToBytes(id, relType2.get, 0, RelationDirection.OUT)
        val end = DistributedKeyConverter.edgeKeyToBytes(id, relType2.get, -1, RelationDirection.OUT)
        ScanOption.newBuilder()
          .setStartKey(ByteString.copyFrom(start))
          .setEndKey(ByteString.copyFrom(end))
          .setLimit(10) // 限制region而不是data
          .build()
      }).asJava
      val res = api.batchScan(opts).asScala.flatMap(f => f.asScala)
      val endNode2 = res.map(kv => {
        ByteUtils.getLong(kv.getKey.toByteArray, 13)
      })
      val country = endNode2.grouped(1000).flatMap(f => api.getNodesByIds(f, labelType)).map(n => n.props(LynxPropertyKey("country")).value.toString)
      val count = country.toSeq.groupBy(f => f).map(f => (f._1, f._2.length))
      count.foreach(println)
    })
  }

  @Test
  def distributionOfCountryOfProject(): Unit = {
    // bigCost: 7170ms
    api.cypher(
      """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy_bioproject]->(b:bioproject)-[:bioproject_country]->(c:map_country)
        |return c.country as country, count(c.country) as num""".stripMargin)
  }

  /**
   * 物种研究国家分布情况(项目)
   */
  @Test
  def distributionOfCountryOfProjectAPI(): Unit = {
    // match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*bioproject.*\)\s*-\[.*\]\s*->\s*\(.*map_country.*\) return.*country.*count\(.* as \S*
    // 94013ms
    val relType = api.getRelationTypeId("taxonomy2bioproject")
    val relType2 = api.getRelationTypeId("bioproject2country")
    val labelType = api.getNodeLabelId("map_country").get
    timeCost(() => {
      var start = System.currentTimeMillis()
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      var endNodeIds = api.findOutRelations(startNode.id.value, relType).map(f => f.endNodeId.value).toSeq
      var cost = System.currentTimeMillis() - start
      println(s"degree 1 cost: ${cost} ms, data size: ${endNodeIds.length}")

      // 72577 ms
      start = System.currentTimeMillis()

      endNodeIds = endNodeIds.sorted
      val opts = endNodeIds.map(id => {
        val start = DistributedKeyConverter.edgeKeyToBytes(id, relType2.get,0, RelationDirection.OUT)
        val end = DistributedKeyConverter.edgeKeyToBytes(id, relType2.get, -1, RelationDirection.OUT)
        ScanOption.newBuilder()
          .setStartKey(ByteString.copyFrom(start))
          .setEndKey(ByteString.copyFrom(end))
          .setLimit(10) // 限制region而不是data
          .build()
      })

      val res1 = opts.grouped(1000).map(range => Future{api.batchScan(seqAsJavaList(range))})
      val res2 = res1.map(f => Await.result(f, Duration.Inf))
      val res3 = res2.toSeq.flatMap(f => f.asScala.map(ff => ff.asScala))
      val res4 = res3.flatMap(f => f.asJava.asScala)
      val length = res4.length

//      val res = api.batchScan(opts).asScala.flatMap(f => f.asScala)
      cost = System.currentTimeMillis() - start
      println(s"degree 2 cost: ${cost} ms, data size: ${length}")

//      println("start 2 degree....")
//      start = System.currentTimeMillis()
//      val res2 = endNodeIds.sorted.grouped(100).map(ids => Future{
//        ids.map(id => api.findOutRelationsEndNodeIds(id, relType2.get))
//      })
//      val res3 = res2.flatMap(f => Await.result(f, Duration.Inf)).filter(f => f.nonEmpty).map(f => f.next())
//      cost = System.currentTimeMillis() - start

      //      start = System.currentTimeMillis()

      //      val endNode2 = res.map(kv => ByteUtils.getLong(kv.getKey.toByteArray, 13))
      //      val country = endNode2.grouped(1000).flatMap(f => api.getNodesByIds(f, labelType)).map(n => n.props(LynxPropertyKey("country")).value.toString)
      //      val count = country.toSeq.groupBy(f => f).map(f => (f._1, f._2.length))
      //      count.foreach(println)
      //      println(s"final cost : ${System.currentTimeMillis() - start}")
    })
  }

  //  @Test
  //  def relativePaperCount(): Unit = {
  //    api.cypher(
  //      """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed) return count(p)""".stripMargin)
  //  }
  //
  //  /**
  //   * 相关资料-论文总数
  //   */
  //  @Test
  //  def relativePaperCountAPI(): Unit = {
  //    // match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed.*\)\s* return count\S*
  //    timeCost(() => {
  //      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
  //      val relType = api.getRelationTypeId("taxonomy2pubmed")
  //      val rels = api.findOutRelations(startNode.id.value, relType)
  //      println(rels.length)
  //    })
  //  }

  @Test
  def relativePaper(): Unit = {
    //223ms
    api.cypher(
      """MATCH (t:taxonomy {tax_id:'9606'})-[r:produce]->(p:pubmed)
        |return p.pubmed_id as pubmed_id, p.title as title, p.authors as authors, p.publish_date as publish_date, p.keywords as keywords SKIP 0 LIMIT 10""".stripMargin)
  }

  /**
   * 相关资料-论文
   */
  @Test
  def relativePaperAPI(): Unit = {
    // match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed.*\)\s* return.*pubmed_id.*title.*authors.*skip \S* limit \S*
    // 1820 ms
    timeCost(() => {
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("produce")
      val endNodeIds = api.findOutRelations(startNode.id.value, relType).map(f => f.endNodeId.value)
      val endNodes = endNodeIds.grouped(1000).flatMap(f => api.getNodesByIds(f, "pubmed"))
      val res = endNodes.map(node => {
        val pubmed_id = node.props(LynxPropertyKey("pubmed_id")).value.toString
        val title = node.props(LynxPropertyKey("title")).value.toString
        val authors = node.props(LynxPropertyKey("authors")).value.toString
        val publish_date = node.props(LynxPropertyKey("publish_date")).value.toString
        val keywords = node.props(LynxPropertyKey("keywords")).value.toString
        (pubmed_id, title, authors, publish_date, keywords)
      })
      println(res.size)
    })
  }

  @Test
  def relativePNG(): Unit = {
    // 592ms
    api.cypher(
      """MATCH (t:taxonomy {tax_id:'9606'})-[r:produce]->(p:pubmed)-[pr:pubmed_png]->(p1:pubmed_pdf_png)
        | return p1.doi as doi, p1.png_path as png_path, p1.caption as caption""".stripMargin)
  }

  /**
   * 相关资料-图片
   */
  @Test
  def relativePNGAPI(): Unit = {
    // match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed.*\)\s* return.*doi.*png.*caption as \S*
    // 5369 ms
    timeCost(() => {
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("produce")
      val relType2 = api.getRelationTypeId("pubmed_png")
      val labelType = api.getNodeLabelId("pubmed_pdf_png").get
      val endNodeIds = api.findOutRelations(startNode.id.value, relType).map(f => f.endNodeId.value).toSeq.sorted

      val opts = endNodeIds.map(id => {
        val start = DistributedKeyConverter.edgeKeyToBytes(id, relType2.get, 0, RelationDirection.OUT)
        val end = DistributedKeyConverter.edgeKeyToBytes(id, relType2.get, -1, RelationDirection.OUT)
        ScanOption.newBuilder()
          .setStartKey(ByteString.copyFrom(start))
          .setEndKey(ByteString.copyFrom(end))
          .setLimit(10) // 限制region而不是data
          .build()
      }).asJava

      val res = api.batchScan(opts).asScala.flatMap(f => f.asScala)
      val endNode2 = res.map(kv => ByteUtils.getLong(kv.getKey.toByteArray, 13))
      val pubmed_pdf_png = endNode2.grouped(1000).flatMap(f => api.getNodesByIds(f, labelType)).map(n => {
        val a = n.props(LynxPropertyKey("doi")).value.toString
        val b = n.props(LynxPropertyKey("png_path")).value.toString
        val c = n.props(LynxPropertyKey("caption")).value.toString
        (a, b, c)
      })
      println(pubmed_pdf_png.length)
    })
  }


  @Test
  def countProject(): Unit = {
    // 272ms
    api.cypher(
      """
        |MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy_bioproject]->(b:bioproject) RETURN count(b) as total
        |""".stripMargin)
  }

  /**
   * 相关资料-项目总数
   */
  @Test
  def countProjectAPI(): Unit = {
    // match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*bioproject.*\)\s* return count\S* as \S*
    // 643ms
    timeCost(() => {
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val relType = api.getRelationTypeId("taxonomy_bioproject")
      val rels = api.countOutRelations(startNode.id.value, relType.get)
      println(rels)
    })
  }

  @Test
  def relativeProject(): Unit = {
    // 404ms
    api.cypher(
      """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2bioproject]->(b:bioproject)
        | RETURN t.scientific_name as scientific_name, b.bioproject_id as bioproject_id, b.title as title, b.cen as cen SKIP 0 LIMIT 10
        |""".stripMargin).show()
  }

  /**
   * 相关资料-项目
   */
  @Test
  def relativeProjectAPI(): Unit = {
    // match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*bioproject.*\)\s* return.*scientific_name.*cen skip \S* limit \S*
    // 569ms
    timeCost(() => {
      val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
      val scientific_name = startNode.props(LynxPropertyKey("scientific_name")).value
      val relType = api.getRelationTypeId("taxonomy2bioproject")
      val endNodeIds = api.findOutRelations(startNode.id.value, relType).map(f => f.endNodeId.value)
      val endNodes = endNodeIds.grouped(1000).flatMap(f => api.getNodesByIds(f, "bioproject"))
      val res = endNodes.map(node => {
        val bioproject_id = node.props(LynxPropertyKey("bioproject_id")).value
        val title = node.props(LynxPropertyKey("title")).value
        val cen = node.props(LynxPropertyKey("cen")).value
        (scientific_name, bioproject_id, title, cen)
      })
      res.slice(0, 10).foreach(println)
    })
  }
}
