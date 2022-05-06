package org.grapheco.pandadb.kernel.distribute.tricks

import org.grapheco.pandadb.kernel.distribute.DistributedGraphFacade
import org.tikv.raw.RawKVClient

import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-04-11 15:35
 */
class BiologyTricks(api: DistributedGraphFacade) {
  val tricksAPI = new BiologyTricksAPI(api)

  private val unknown1 = Pattern.compile("""match\s*\(.*taxonomy\)\s*where .*tax_id\s*=\s*'.*'\s*return \S as \S*""")
  private val unknown2 = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*return \S* as scientific_name""")
  private val unknown3 = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\[.*produce\].*\(.*pubmed\)\swhere.*>.*publish_date.*return count\(.*\) as num""")
  private val unknown4 = Pattern.compile("""match.*>=.*unwind.*;.*match.*>=.*return distinct.*year, k, count.*order by year asc;""")
  private val unknownF5 = Pattern.compile("""match\s*\(.*taxonomy.*\).*-\[.*pubmed\s*\) return sum.*cited_num.* as num""")
  private val unknownF7 = Pattern.compile("""match\s*\(.*taxonomy.*\).*-\[.*pubmed\s*\) where.*>.*publish_date.*return sum.*cited_num.* as num""")

  private val basicInfoOfTaxonomy = Pattern.compile("""match\s*\(.*\)\s*where\s*\S*\s*=\s*'\S*' return\s*n""")
  // 4, 18
  private val totalPubmedOfTaxonomy = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed\s*\)\s* return count\S* as \S*""")
  // 5, 7
  //  private val countCited = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return sum\s*\(\s*tointeger.*\) as \S*""")
  // no
  //  private val count3YearCited = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*where \S*\s*<\s*tointeger.*\) return count\S* as \S*""")
  private val earliest = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return min.*\) as \S*""")
  private val findTop3LevelParent = Pattern.compile("""match \S*=.*\[.*1..3\].* return \S*""")
  private val projectInfo = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return \S*.title.*bioproject_id.*smdt.*limit \S*""")
  private val geneOfTaxonomy = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return.*title.*gene_id limit \S*""")
  private val genomeOfTaxonomy = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return.*genome_id.*aacc limit \S*""")
  private val paperTendency = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where tointeger.*>=\s*\S* return .*publish_date.*count\S* as \S* order by \S* \S*""")
  // no
  //  private val topKTendency = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where.*>= \S*.*<= \S* unwind.*>=.*<=.* return.*publish_date.*count\S* as \S* order by \S* \S*""")
  private val keywordRelation = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where.*with \S* as \S*.*unwind.*<>.*limit \S*""")
  private val countKey = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where.* with.*unwind \S* as \S* return .*count.*limit \S*""")
  private val distributionOfCountryOfPaper = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed.*\)\s*-\[.*\]\s*->\s*\(.*map_country.*\) return.*country.*count\(.* as \S*""")
  // no
  //  private val distributionOfCountryOfProject = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*bioproject.*\)\s*-\[.*\]\s*->\s*\(.*map_country.*\) return.*country.*count\(.* as \S*""")
  private val relativePaper = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed.*\)\s* return.*pubmed_id.*title.*authors.*skip \S* limit \S*""")
  private val relativePNG = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed.*\)\s* return.*doi.*png.*caption as \S*""")
  private val countProject = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*bioproject.*\)\s* return count\S* as \S*""")
  private val relativeProject = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*bioproject.*\)\s* return.*scientific_name.*cen skip \S* limit \S*""")

  private val fulltext = Pattern.compile("""call.*fulltext.*""")

  def returnCypherParams(cypher: String): BioDataFrame = {
    val res = parseCypherParams(cypher)
    res._1.head match {
      case "fulltext" => {
        val text = res._1(1)
        val schema = res._2
        tricksAPI.fulltext(text, schema)
      }
      case "unknown1" => {
        val taxId = res._1(1)
        val schema = res._2
        tricksAPI.unknown1(taxId, schema)
      }
      case "unknown2" => {
        val taxId = res._1(1)
        val schema = res._2
        tricksAPI.unknown2(taxId, schema)
      }
      case "unknown3" => {
        val taxId = res._1(1)
        val year = res._1(2)
        val schema = res._2
        tricksAPI.unknown3(taxId, year.toInt, schema)
      }
      case "unknown4" => {
        val taxId = res._1(1)
        val year = res._1(3)
        val limit = res._1.last
        val schema = res._2
        tricksAPI.unknown4(taxId, year.toInt, limit.toInt, schema)
      }
      case "unknownF5" => {
        val taxId = res._1(1)
        val schema = res._2
        tricksAPI.unknownF5(taxId, schema)
      }
      case "unknownF7" => {
        val taxId = res._1(1)
        val year = res._1(2)
        val schema = res._2
        tricksAPI.unknownF7(taxId, year.toInt, schema)
      }
      case "basicInfoOfTaxonomy" => {
        val taxId = res._1(1)
        val schema = res._2
        tricksAPI.basicInfoOfTaxonomy(taxId, schema)
      }
      case "totalPubmedOfTaxonomy" => {
        val taxId = res._1(1)
        val schema = res._2
        tricksAPI.totalPubmedOfTaxonomy(taxId, schema)
      }
      case "countCited" => {
        val taxId = res._1(1)
        val schema = res._2
        tricksAPI.countCited(taxId, schema)
      }
      case "count3YearCited" => {
        val taxId = res._1(1)
        val year = res._1(2)
        val schema = res._2
        tricksAPI.count3YearCited(taxId, year.toInt, schema)
      }
      case "earliest" => {
        val taxId = res._1(1)
        val schema = res._2
        tricksAPI.earliest(taxId, schema)
      }
      case "findTop3LevelParent" => {
        val taxId = res._1(1)
        val schema = res._2
        tricksAPI.findTop3LevelParent(taxId, schema)
      }
      case "projectInfo" => {
        val taxId = res._1(1)
        val limit = res._1(2)
        val schema = res._2
        tricksAPI.projectInfo(taxId, limit.toInt, schema)
      }
      case "geneOfTaxonomy" => {
        val taxId = res._1(1)
        val limit = res._1(2)
        val schema = res._2
        tricksAPI.geneOfTaxonomy(taxId, limit.toInt, schema)
      }
      case "genomeOfTaxonomy" => {
        val taxId = res._1(1)
        val limit = res._1(2)
        val schema = res._2
        tricksAPI.genomeOfTaxonomy(taxId, limit.toInt, schema)
      }
      case "paperTendency" => {
        val taxId = res._1(1)
        val schema = res._2
        tricksAPI.paperTendency(taxId, schema)
      }
      case "topKTendency" => {
        val taxId = res._1(1)
        val startYear = res._1(3)
        val endYear = res._1(5)
        val limit = res._1(7)
        val schema = res._2
        tricksAPI.topKTendency(taxId, startYear.toInt, endYear.toInt, limit.toInt, schema)
      }
      case "keywordRelation" => {
        val taxId = res._1(1)
        val limit = res._1(2)
        val schema = res._2
        tricksAPI.keywordRelation(taxId, limit.toInt, schema)
      }
      case "countKey" => {
        val taxId = res._1(1)
        val limit = res._1(2)
        val schema = res._2
        tricksAPI.countKey(taxId, limit.toInt, schema)
      }
      case "distributionOfCountryOfPaper" => {
        val taxId = res._1(1)
        val schema = res._2
        tricksAPI.distributionOfCountryOfPaper(taxId, schema)
      }
      case "distributionOfCountryOfProject" => {
        val taxId = res._1(1)
        val schema = res._2
        tricksAPI.distributionOfCountryOfProject(taxId, schema)
      }
      case "relativePaper" => {
        val taxId = res._1(1)
        val skip = res._1(2)
        val limit = res._1(3)
        val schema = res._2
        tricksAPI.relativePaper(taxId, skip.toInt, limit.toInt, schema)
      }
      case "relativePNG" => {
        val taxId = res._1(1)
        val schema = res._2
        tricksAPI.relativePNG(taxId, schema)
      }
      case "countProject" => {
        val taxId = res._1(1)
        val schema = res._2
        tricksAPI.countProject(taxId, schema)
      }
      case "relativeProject" => {
        val taxId = res._1(1)
        val skip = res._1(2)
        val limit = res._1(3)
        val schema = res._2
        tricksAPI.relativeProject(taxId, skip.toInt, limit.toInt, schema)
      }
    }
  }

  def parseCypherParams(_cypher: String): (Seq[String], Seq[String]) = {
    val cypher = _cypher.toLowerCase.replaceAll("\r", " ").replaceAll("\n", " ").trim
    val result = {
      if (basicInfoOfTaxonomy.matcher(cypher).matches()) {
        val p = Pattern.compile("""tax_id\s*=\s*\S*""")
        val res = p.matcher(cypher)
        res.find()
        val target = res.group()
        (Seq("basicInfoOfTaxonomy", target.slice(target.length - 5, target.length - 1)), getReturnSchema(cypher))
      }
      else if (fulltext.matcher(cypher).matches()){
        val p = Pattern.compile("""\*\S*\*""")
        val res = p.matcher(cypher)
        res.find()
        val target = res.group()
        (Seq("fulltext", target), Seq("node"))
      }
      else if (totalPubmedOfTaxonomy.matcher(cypher).matches()) {
        val taxId = getTaxId(cypher)
        (Seq("totalPubmedOfTaxonomy") ++ taxId, getReturnSingleAsSchema(cypher))
      }
      else if (unknown1.matcher(cypher).matches()){
        val p = Pattern.compile("""tax_id\s*=\s*\S*""")
        val res = p.matcher(cypher)
        res.find()
        val target = res.group()
        (Seq("unknown1", target.slice(target.length - 5, target.length - 1)), getReturnSingleAsSchema(cypher))
      }
      else if (unknown2.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        (Seq("unknown2") ++ taxId, getReturnSingleAsSchema(cypher))
      }
      else if (unknown3.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        val year = getStartYearNoEqual(cypher)
        (Seq("unknown3") ++ taxId ++ year, getReturnSingleAsSchema(cypher))
      }
      else if (unknown4.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        val year = getStartYear(cypher)
        val limit = getLimit(cypher)
        (Seq("unknown4") ++ taxId ++ year ++ Seq(limit), Seq("year", "k", "num"))
      }
      else if (unknownF5.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        (Seq("unknownF5") ++ taxId, getReturnSingleAsSchema(cypher))
      }
      else if (unknownF7.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        val year = getStartYearNoEqual(cypher)
        (Seq("unknownF7") ++ taxId ++ year, getReturnSingleAsSchema(cypher))
      }
//      else if (countCited.matcher(cypher).matches()) {
//        val taxId = getTaxId(cypher)
//        (Seq("countCited") ++ taxId, getReturnSingleAsSchema(cypher))
//      }
//      else if (count3YearCited.matcher(cypher).matches()) {
//        val taxId = getTaxId(cypher)
//        val p = Pattern.compile("""where \S*""")
//        val res = p.matcher(cypher)
//        res.find()
//        val target = res.group()
//        val year = target.slice(target.length - 4, target.length)
//        (Seq("count3YearCited") ++ taxId ++ Seq(year), getReturnSingleAsSchema(cypher))
//      }
      else if (earliest.matcher(cypher).matches()) {
        val taxId = getTaxId(cypher)
        (Seq("earliest") ++ taxId, getReturnSingleAsSchema(cypher))
      }
      else if (findTop3LevelParent.matcher(cypher).matches()) {
        val taxId = getTaxId(cypher)
        (Seq("findTop3LevelParent") ++ taxId, getReturnSchema(cypher))
      }
      else if (projectInfo.matcher(cypher).matches()) {
        val taxId = getTaxId(cypher)
        val limit = getLimit(cypher)
        (Seq("projectInfo") ++ taxId ++ Seq(limit), getReturnMultipleAsSchema(cypher))
      }
      else if (geneOfTaxonomy.matcher(cypher).matches()) {
        val taxId = getTaxId(cypher)
        val limit = getLimit(cypher)
        (Seq("geneOfTaxonomy") ++ taxId ++ Seq(limit), getReturnMultipleAsSchema(cypher))
      }
      else if (genomeOfTaxonomy.matcher(cypher).matches()) {
        val taxId = getTaxId(cypher)
        val limit = getLimit(cypher)
        (Seq("genomeOfTaxonomy") ++ taxId ++ Seq(limit), getReturnMultipleAsSchema(cypher))
      }
      else if (paperTendency.matcher(cypher).matches()) {
        val taxId = getTaxId(cypher)
        (Seq("paperTendency") ++ taxId, getReturnMultipleAsSchema(cypher))
      }
//      else if (topKTendency.matcher(cypher).matches()) {
//        val taxId = getTaxId(cypher)
//        val startYear = getStartYear(cypher)
//        val endYear = getEndYear(cypher)
//        val limit = getLimit(cypher)
//        (Seq("topKTendency") ++ taxId ++ startYear ++ endYear ++ Seq(limit), Seq("year", "k", "num"))
//      }
      else if (keywordRelation.matcher(cypher).matches()) {
        val taxId = getTaxId(cypher)
        val limit = getLimit(cypher)
        (Seq("keywordRelation") ++ taxId ++ Seq(limit), getReturnMultipleAsSchema(cypher))
      }
      else if (countKey.matcher(cypher).matches()) {
        val taxId = getTaxId(cypher)
        val limit = getLimit(cypher)
        (Seq("countKey") ++ taxId ++ Seq(limit), Seq("keyword", "num"))
      }
      else if (distributionOfCountryOfPaper.matcher(cypher).matches()) {
        val taxId = getTaxId(cypher)
        (Seq("distributionOfCountryOfPaper") ++ taxId, getReturnMultipleAsSchema(cypher))
      }
//      else if (distributionOfCountryOfProject.matcher(cypher).matches()) {
//        val taxId = getTaxId(cypher)
//        (Seq("distributionOfCountryOfProject") ++ taxId, getReturnMultipleAsSchema(cypher))
//      }
      else if (relativePaper.matcher(cypher).matches()) {
        val taxId = getTaxId(cypher)
        val skip = getSkip(cypher)
        val limit = getLimit(cypher)
        (Seq("relativePaper") ++ taxId ++ Seq(skip, limit), getReturnMultipleAsSchema(cypher))
      }
      else if (relativePNG.matcher(cypher).matches()) {
        (Seq("relativePNG") ++ getTaxId(cypher), getReturnMultipleAsSchema(cypher))
      }
      else if (countProject.matcher(cypher).matches()) {
        (Seq("countProject") ++ getTaxId(cypher), getReturnSingleAsSchema(cypher))
      }
      else if (relativeProject.matcher(cypher).matches()) {
        val taxId = getTaxId(cypher)
        val skip = getSkip(cypher)
        val limit = getLimit(cypher)
        (Seq("relativeProject") ++ taxId ++ Seq(skip, limit), getReturnMultipleAsSchema(cypher))
      }
      else (Seq.empty[String], Seq.empty[String])
    }
    result
  }

  private def getTaxId(str: String): Seq[String] = {
    val p = Pattern.compile("""tax_id\s*:\s*'\S*'""")
    val res = p.matcher(str)
    val targets = ArrayBuffer[String]()
    while (res.find()) {
      val target = res.group()
      targets.append(target.slice(target.length - 5, target.length - 1))
    }
    targets.toSeq
  }

  private def getLimit(str: String): String = {
    val p = Pattern.compile("""limit \S*""")
    val res = p.matcher(str)
    res.find()
    val target = res.group()
    target.slice(6, target.length).trim
  }

  private def getStartYear(str: String): Seq[String] = {
    val p = Pattern.compile(""">=\s*\S*""")
    val res = p.matcher(str)
    val targets = ArrayBuffer[String]()
    while (res.find()) {
      val target = res.group()
      targets.append(target.slice(target.length - 4, target.length))
    }
    targets
  }
  private def getStartYearNoEqual(str: String): Seq[String] = {
    val p = Pattern.compile("""\S*\s*> """)
    val res = p.matcher(str)
    val targets = ArrayBuffer[String]()
    while (res.find()) {
      val target = res.group()
      targets.append(target.slice(0, 4))
    }
    targets
  }

  private def getEndYear(str: String): Seq[String] = {
    val p = Pattern.compile("""<=\s*\S*""")
    val res = p.matcher(str)
    val targets = ArrayBuffer[String]()
    while (res.find()) {
      val target = res.group()
      targets.append(target.slice(target.length - 4, target.length))
    }
    targets
  }

  private def getSkip(str: String): String = {
    val p = Pattern.compile("""skip \S*""")
    val res = p.matcher(str)
    res.find()
    val target = res.group()
    target.slice(5, target.length).trim
  }

  private def getReturnSchema(str: String): Seq[String] = {
    val p = Pattern.compile("""return \S*""")
    val res = p.matcher(str)
    res.find()
    val target = res.group()
    Seq(target.slice(7, target.length).trim)
  }

  private def getReturnSingleAsSchema(str: String): Seq[String] = {
    val p = Pattern.compile("""return.*as \S*""")
    val res = p.matcher(str)
    res.find()
    val target = res.group()
    Seq(target.split("as").last.trim)
  }

  private def getReturnMultipleAsSchema(str: String): Seq[String] = {
    val p = Pattern.compile("""return.*as \S*""")
    val res = p.matcher(str)
    res.find()
    val target = res.group()
    val p2 = Pattern.compile("""as \S*""")
    val r2 = p2.matcher(target)
    val schema = ArrayBuffer[String]()
    while (r2.find()) {
      val res = r2.group().split(",")
      val v = res.head
      schema.append(v.slice(2, v.length).trim)
    }
    schema
  }
}
