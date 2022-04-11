package org.grapheco.pandadb.kv.distributed

import org.grapheco.pandadb.kernel.distribute.tricks.BiologyTricks

import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-04-11 08:56
 */
object BioTricksTest {
  val basicInfoOfTaxonomy = Pattern.compile("""match\s*\(.*\)\s*where\s*\S*\s*=\s*'\S*' return\s*n""")
  val totalPubmedOfTaxonomy = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed\s*\)\s* return count\S* as \S*""")
  val countCited = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return sum\s*\(\s*tointeger.*\) as \S*""")
  val count3YearCited = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*where \S*\s*<\s*tointeger.*\) return count\S* as \S*""")
  val earliest = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return min.*\) as \S*""")
  val findTop3LevelParent = Pattern.compile("""match \S*=.*\[.*1..3\].* return \S*""")
  val projectInfo = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return \S*.title.*bioproject_id.*smdt.*limit \S*""")
  val geneOfTaxonomy = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return.*title.*gene_id limit \S*""")
  val genomeOfTaxonomy = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return.*genome_id.*aacc limit \S*""")
  val paperTendency = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where tointeger.*>=\s*\S* return .*publish_date.*count\S* as \S* order by \S* \S*""")
  val topKTendency = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where.*>= \S*.*<= \S* unwind.*>=.*<=.* return.*publish_date.*count\S* as \S* order by \S* \S*""")
  val keywordRelation = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where.*with \S* as \S*.*unwind.*<>.*limit \S*""")
  val countKey = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where.* with.*unwind \S* as \S* return .*count.*limit \S*""")
  val distributionOfCountryOfPaper = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed.*\)\s*-\[.*\]\s*->\s*\(.*map_country.*\) return.*country.*count\(.* as \S*""")
  val distributionOfCountryOfProject = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*bioproject.*\)\s*-\[.*\]\s*->\s*\(.*map_country.*\) return.*country.*count\(.* as \S*""")
  val relativePaper = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed.*\)\s* return.*pubmed_id.*title.*authors.*skip \S* limit \S*""")
  val relativePNG = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed.*\)\s* return.*doi.*png.*caption as \S*""")
  val countProject = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*bioproject.*\)\s* return count\S* as \S*""")
  val relativeProject = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*bioproject.*\)\s* return.*scientific_name.*cen skip \S* limit \S*""")
  val tool = new BiologyTricks

  val cyphers = List(
    // basicInfoOfTaxonomy
    """MATCH (n: taxonomy) where n.tax_id ='9606' RETURN n""",
    // totalPubmedOfTaxonomy
    """MATCH (t:taxonomy{tax_id:'9606'})-[:taxonomy2pubmed]->(p:pubmed) RETURN count(p) as num""",
    // countCited
    """MATCH (t:taxonomy{tax_id:'9606'})-[:taxonomy2pubmed]->(p:pubmed) RETURN sum(toInteger(p.cited_num)) as num""",
    // count3YearCited
    """MATCH (t:taxonomy{tax_id:'9606'})-[:taxonomy2pubmed]->(p:pubmed) where 2019 < toInteger(left(p.publish_date, 4)) RETURN count(p) as num""",
    // earliest
    """MATCH (t:taxonomy{tax_id:'9606'})-[:taxonomy2pubmed]->(p:pubmed) RETURN min(left(p.publish_date, 4)) as year""",
    // findTop3LevelParent
    """match t=(t0:taxonomy{tax_id:'9606'})-[r:parent*1..3]->(t2:taxonomy) return t""",
    // projectInfo
    """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2bioproject]->(b:bioproject) return b.title as title, b.bioproject_id as bioproject_id order by b.smdt desc limit 20""",
    // geneOfTaxonomy
    """MATCH (t:taxonomy{tax_id:'9606'})-[r:taxonomy2gene]->(g:gene) RETURN g.title as title, g.gene_id as gene_id limit 20""",
    // genomeOfTaxonomy
    """MATCH (t:taxonomy{tax_id:'9606'})-[r:taxonomy2genome]->(g:genome) RETURN g.genome_id as genome_id, g.aacc as aacc limit 20""",
    // paperTendency
    """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed) where toInteger(left(p.publish_date, 4)) >= 1980
      |RETURN distinct left(p.publish_date, 4) as year , count(p) as num order by year asc""".stripMargin,
    // topKTendency
    """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed)
      |where p.keywords is not null and
      |toInteger(left(p.publish_date,4)) >= 1970  and toInteger(left(p.publish_date,4)) <= 2021
      |unwind(split(toLower(p.keywords),';')) as keyword with distinct keyword as k,
      |count(keyword) as num order by num desc limit 20
      |match (t:taxonomy {tax_id:'9606'})-[:taxonomy2pubmed]->(p:pubmed)
      |where p.keywords is not null and toInteger(left(p.publish_date,4)) >= 1970 and toInteger(left(p.publish_date,4)) <= 2021 and
      |k in split(toLower(p.keywords),';')
      |return distinct left(p.publish_date,4) as year, k, count(p) as num order by year asc""".stripMargin,
    // keywordRelation
    """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed)
      |where p.keywords is not null with (split(toLower(p.keywords),';')) as keywords
      |with keywords as keywords1, keywords as keywords2
      |unwind keywords1 as keyword1 unwind keywords2 as keyword2
      |with (keyword1 + ';' + keyword2) as keyword where keyword1 <> keyword2
      |return distinct keyword as keyword, count(keyword) as num order by num desc limit 10""".stripMargin,
    // countKey
    """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed)
      |where p.keywords is not null with (split(toLower(p.keywords),';')) as keywords
      |unwind keywords as keyword
      |return distinct keyword, count(keyword) as num order by num desc limit 10""".stripMargin,
    // distributionOfCountryOfPaper
    """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed)-[:pubmed2country]->(c:map_country)
      |RETURN c.country as country, count(c.country) as num""".stripMargin,
    // distributionOfCountryOfProject
    """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2bioproject]->(b:bioproject)-[:bioproject2country]->(c:map_country)
      |return c.country as country, count(c.country) as num""".stripMargin,
    // relativePaper
    """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed)
      |return p.pubmed_id as pubmed_id, p.title as title, p.authors as authors, p.publish_date as publish_date, p.keywords as keywords SKIP 0 LIMIT 10""".stripMargin,
    // relativePNG
    """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2pubmed]->(p:pubmed)-[pr:pubmed2pubmed_pdf_png]->(p1:pubmed_pdf_png)
      | return p1.doi as doi, p1.png_path as png_path, p1.caption as caption""".stripMargin,
    // countProject
    """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2bioproject]->(b:bioproject) RETURN count(b) as total""",
    // relativeProject
    """MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2bioproject]->(b:bioproject)
      | RETURN t.scientific_name as scientific_name, b.bioproject_id as bioproject_id, b.title as title, b.cen as cen SKIP 0 LIMIT 10""".stripMargin
  )


  def main(args: Array[String]): Unit = {
    val c = cyphers.map(f => f.replaceAll("\r", " ").replaceAll("\n", " ").toLowerCase())
    c.foreach(cc => {
      val res = tool.parseCypherParams(cc)
      println(res)
    })
  }
}
