package org.grapheco.pandadb.kv.distributed

import org.grapheco.pandadb.kernel.distribute.DistributedGraphFacade
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import org.junit.{After, Before, Test}
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.raw.RawKVClient

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-03-28 18:36
 */
class BiologyTest {
  var api: DistributedGraphFacade = _

  var tikv: RawKVClient = _

  val kvHosts = "10.0.82.144:2379,10.0.82.145:2379,10.0.82.146:2379"
  val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
  val udpClient = Array(new UDPClient("127.0.0.1", 6000))
  @Before
  def init(): Unit = {
    val conf = TiConfiguration.createRawDefault(kvHosts)
    val session = TiSession.create(conf)
    tikv = session.createRawClient()
    api = new DistributedGraphFacade(kvHosts, indexHosts, new UDPClientManager(udpClient))
  }
  @After
  def close(): Unit = {
    api.close()
  }
  @Test
  def basicInfo(): Unit ={
    api.cypher("""MATCH (n:taxonomy) where n.tax_id ='9606' RETURN n""")
  }
  @Test
  def taxId2scientificName(): Unit ={
    api.cypher("""MATCH (n:taxonomy {tax_id:'9606'}) RETURN n.scientific_name as scientific_name""")
  }
  @Test
  def totalPubmedOfTaxonomy(): Unit ={
    api.cypher("""MATCH (t:taxonomy{tax_id:'9606'})-[:taxonomy2pubmed]->(p:pubmed) RETURN count(p) as num""")
  }
  @Test
  def countCited(): Unit ={
    api.cypher("""MATCH (t:taxonomy{tax_id:'9606'})-[:taxonomy2pubmed]->(p:pubmed) RETURN sum(toInteger(p.cited_num)) as num""")
  }
  @Test
  def count3YearCited(): Unit ={
    api.cypher("""MATCH (t:taxonomy{tax_id:'9606'})-[:taxonomy2pubmed]->(p:pubmed) where 1970 > toInteger(left(p.publish_date, 4)) RETURN count(p) as num""")
  }
  @Test
  def earliest(): Unit ={
    api.cypher("""MATCH (t:taxonomy{tax_id:'9606'})-[:taxonomy2pubmed]->(p:pubmed) RETURN min(left(p.publish_date, 4)) as year""")
  }
  @Test
  def projectInfo(): Unit ={
    api.cypher("""MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2bioproject]->(b:bioproject) return b.title as title, b.bioproject_id as bioproject_id order by b.mdate desc limit 20""")
  }
  @Test
  def geneOfTaxonomy(): Unit ={
    api.cypher("""MATCH (t:taxonomy{tax_id:'9606'})-[r:taxonomy2gene]->(g:gene) RETURN g.title as title, g.gene_id as gene_id limit 20""")
  }
  @Test
  def genomeOfTaxonomy(): Unit ={
    api.cypher("""MATCH (t:taxonomy{tax_id:'9606'})-[r:taxonomy2genome]->(g:genome) RETURN g.genome_id as genome_id, g.aacc as aacc limit 20""")
  }
  @Test
  def paperTendency(): Unit ={
    api.cypher("""MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed) where toInteger(left(p.publish_date, 4)) >= 1980 RETURN distinct left(p.publish_date, 4) as year , count(p) as num order by year asc""")
  }
  @Test
  def topKTendency(): Unit ={
    api.cypher("""MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed) where p.keywords is not null and toInteger(left(p.publish_date,4)) >= 1970  and toInteger(left(p.publish_date,4)) <= 2021 unwind(split(toLower(p.keywords),';')) as keyword with distinct keyword as k, count(keyword) as num order by num desc limit 20 match (t:taxonomy {tax_id:'9606'})-[:taxonomy2pubmed]->(p:pubmed) where p.keywords is not null and toInteger(left(p.publish_date,4)) >= 1970  and toInteger(left(p.publish_date,4)) <= 2021 and k in split(toLower(p.keywords),';') return distinct left(p.publish_date,4) as year, k, count(p) as num order by year asc;""")
  }
  @Test
  def keyRelation(): Unit ={
    api.cypher("""MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed) where p.keywords is not null with (split(toLower(p.keywords),';')) as keywords with keywords as keywords1, keywords as keywords2 unwind keywords1 as keyword1 unwind keywords2 as keyword2 with (keyword1 + ';' + keyword2) as keyword where keyword1 <> keyword2 return distinct keyword as keyword, count(keyword) as num order by num desc limit 10""")
  }
  @Test
  def countKey(): Unit ={
    api.cypher("""MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed) where p.keywords is not null with (split(toLower(p.keywords),';')) as keywords unwind keywords as keyword return distinct keyword, count(keyword) as num order by num desc limit 10""")
  }
  @Test
  def distributionOfCountryOfPaper(): Unit ={
    api.cypher("""MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed)-[pubmed2country]->(c:map_country) RETURN c.country as country, count(c.country) as num""")
  }
  @Test
  def distributionOfCountryOfProject(): Unit ={
    api.cypher("""MATCH (t:taxonomy {tax_id:'9606'})-[taxonomy2bioproject]->(b:bioproject)-[:bioproject2country]->(c:map_country) return c.country as country, count(c.country) as num""")
  }
  @Test
  def relativePaper(): Unit ={
    api.cypher("""MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed) return p.pubmed_id as pubmed_id, p.title as title, p.authors as authors, p.publish_date as publish_date, p.keywords as keywords SKIP 0 LIMIT 10""")
  }
  @Test
  def relativePNG(): Unit ={
    api.cypher("""MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed) return p.doi as doi, p.png_path as png_path""")
  }
  @Test
  def relativeProject(): Unit ={
    api.cypher(
      """
        |MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2bioproject]->(b:bioproject) RETURN t.scientific_name as scientific_name, b.bioproject_id as bioproject_id, b.title as title, b.cen as cen SKIP 0 LIMIT 10
        |""".stripMargin).show()
  }
}
