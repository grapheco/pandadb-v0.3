package org.grapheco.pandadb.kernel.distribute.tricks

import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-04-11 15:35
 */
class BiologyTricks {
  private val basicInfoOfTaxonomy = Pattern.compile("""match\s*\(.*\)\s*where\s*\S*\s*=\s*'\S*' return\s*n""")
  private val totalPubmedOfTaxonomy = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed\s*\)\s* return count\S* as \S*""")
  private val countCited = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return sum\s*\(\s*tointeger.*\) as \S*""")
  private val count3YearCited = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*where \S*\s*<\s*tointeger.*\) return count\S* as \S*""")
  private val earliest = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return min.*\) as \S*""")
  private val findTop3LevelParent = Pattern.compile("""match \S*=.*\[.*1..3\].* return \S*""")
  private val projectInfo = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return \S*.title.*bioproject_id.*smdt.*limit \S*""")
  private val geneOfTaxonomy = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return.*title.*gene_id limit \S*""")
  private val genomeOfTaxonomy = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s*return.*genome_id.*aacc limit \S*""")
  private val paperTendency = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where tointeger.*>=\s*\S* return .*publish_date.*count\S* as \S* order by \S* \S*""")
  private val topKTendency = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where.*>= \S*.*<= \S* unwind.*>=.*<=.* return.*publish_date.*count\S* as \S* order by \S* \S*""")
  private val keywordRelation = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where.*with \S* as \S*.*unwind.*<>.*limit \S*""")
  private val countKey = Pattern.compile("""match\s*\(.*\)\s*-\s*\[.*]\s*->\s*\(.*\)\s* where.* with.*unwind \S* as \S* return .*count.*limit \S*""")
  private val distributionOfCountryOfPaper = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed.*\)\s*-\[.*\]\s*->\s*\(.*map_country.*\) return.*country.*count\(.* as \S*""")
  private val distributionOfCountryOfProject = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*bioproject.*\)\s*-\[.*\]\s*->\s*\(.*map_country.*\) return.*country.*count\(.* as \S*""")
  private val relativePaper = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed.*\)\s* return.*pubmed_id.*title.*authors.*skip \S* limit \S*""")
  private val relativePNG = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*pubmed.*\)\s* return.*doi.*png.*caption as \S*""")
  private val countProject = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*bioproject.*\)\s* return count\S* as \S*""")
  private val relativeProject = Pattern.compile("""match\s*\(.*taxonomy.*\)\s*-\s*\[.*]\s*->\s*\(.*bioproject.*\)\s* return.*scientific_name.*cen skip \S* limit \S*""")


  def returnCypherParams(_cypher: String): Unit = {
    val cypher = _cypher.toLowerCase.replaceAll("\r", " ").replaceAll("\n", " ")
    val res = parseCypherParams(cypher)
    res._1.head match {
      case "basicInfoOfTaxonomy" => {

      }
      case "totalPubmedOfTaxonomy" => {

      }
      case "countCited" => {

      }
      case "count3YearCited" => {

      }
      case "earliest" => {

      }
      case "findTop3LevelParent" => {

      }
      case "projectInfo" => {

      }
      case "geneOfTaxonomy" => {

      }
      case "genomeOfTaxonomy" => {

      }
      case "paperTendency" => {

      }
      case "topKTendency" => {

      }
      case "keywordRelation" => {

      }
      case "countKey" => {

      }
      case "distributionOfCountryOfPaper" => {

      }
      case "distributionOfCountryOfProject" => {

      }
      case "relativePaper" => {

      }
      case "relativePNG" => {

      }
      case "countProject" => {

      }
      case "relativeProject" => {

      }
    }
  }

  def parseCypherParams(cypher: String): (Seq[String], Seq[String]) ={
    val result = {
      if (basicInfoOfTaxonomy.matcher(cypher).matches()){
        val p = Pattern.compile("""tax_id\s*=\s*\S*""")
        val res = p.matcher(cypher)
        res.find()
        val target = res.group()
        (Seq("basicInfoOfTaxonomy", target.slice(target.length - 5, target.length - 1)), getReturnSchema(cypher))
      }
      else if (totalPubmedOfTaxonomy.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        (Seq("totalPubmedOfTaxonomy") ++ taxId, getReturnSingleAsSchema(cypher))
      }
      else if (countCited.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        (Seq("countCited") ++ taxId, getReturnSingleAsSchema(cypher))
      }
      else if (count3YearCited.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        val p = Pattern.compile("""where \S*""")
        val res = p.matcher(cypher)
        res.find()
        val target = res.group()
        val year = target.slice(target.length - 4, target.length)
        (Seq("count3YearCited") ++ taxId ++ Seq(year), getReturnSingleAsSchema(cypher))
      }
      else if (earliest.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        (Seq("earliest") ++ taxId, getReturnSingleAsSchema(cypher))
      }
      else if (findTop3LevelParent.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        (Seq("findTop3LevelParent") ++ taxId, getReturnSchema(cypher))
      }
      else if (projectInfo.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        val limit = getLimit(cypher)
        (Seq("projectInfo")  ++ taxId ++ Seq(limit), getReturnMultipleAsSchema(cypher))
      }
      else if (geneOfTaxonomy.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        val limit = getLimit(cypher)
        (Seq("geneOfTaxonomy")  ++ taxId ++ Seq(limit), getReturnMultipleAsSchema(cypher))
      }
      else if (genomeOfTaxonomy.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        val limit = getLimit(cypher)
        (Seq("genomeOfTaxonomy")  ++ taxId ++ Seq(limit), getReturnMultipleAsSchema(cypher))
      }
      else if (paperTendency.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        (Seq("paperTendency") ++ taxId, getReturnMultipleAsSchema(cypher))
      }
      else if (topKTendency.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        val startYear = getStartYear(cypher)
        val endYear = getEndYear(cypher)
        val limit = getLimit(cypher)
        (Seq("topKTendency") ++ taxId ++ startYear ++ endYear ++ Seq(limit), Seq("year", "k", "num"))
      }
      else if (keywordRelation.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        val limit = getLimit(cypher)
        (Seq("keywordRelation") ++ taxId ++ Seq(limit), getReturnMultipleAsSchema(cypher))
      }
      else if (countKey.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        val limit = getLimit(cypher)
        (Seq("countKey") ++ taxId ++ Seq(limit), Seq("keyword", "num"))
      }
      else if (distributionOfCountryOfPaper.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        (Seq("distributionOfCountryOfPaper") ++ taxId, getReturnMultipleAsSchema(cypher))
      }
      else if (distributionOfCountryOfProject.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        (Seq("distributionOfCountryOfProject") ++ taxId, getReturnMultipleAsSchema(cypher))
      }
      else if (relativePaper.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        val skip = getSkip(cypher)
        val limit = getLimit(cypher)
        (Seq("relativePaper") ++ taxId ++ Seq(skip, limit), getReturnMultipleAsSchema(cypher))
      }
      else if (relativePNG.matcher(cypher).matches()){
        (Seq("relativePNG") ++ getTaxId(cypher), getReturnMultipleAsSchema(cypher))
      }
      else if (countProject.matcher(cypher).matches()){
        (Seq("countProject") ++ getTaxId(cypher), getReturnSingleAsSchema(cypher))
      }
      else if (relativeProject.matcher(cypher).matches()){
        val taxId = getTaxId(cypher)
        val skip = getSkip(cypher)
        val limit = getLimit(cypher)
        (Seq("relativeProject")  ++ taxId ++ Seq(skip, limit), getReturnMultipleAsSchema(cypher))
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
  private def getEndYear(str: String): Seq[String] = {
    val p = Pattern.compile("""<=\s*\S*""")
    val res = p.matcher(str)
    val targets = ArrayBuffer[String]()
    while (res.find()){
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
    while (r2.find()){
      val res = r2.group().split(",")
      val v =  res.head
      schema.append(v.slice(2, v.length).trim)
    }
    schema
  }
}
