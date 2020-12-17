package cn.pandadb.kv.performance

import org.junit.Test

import scala.collection.mutable
import scala.util.Random

class QueryTemplate {
  def getCypherCreateNode(id: Long, idStr: String, flag: Boolean, labels: Array[String] = Array()): String = {
    var labelStr = new StringBuilder()
    labels.foreach(l => {labelStr ++= s":${l}"})
    s"create(n${labelStr}:{id_p: ${id}, idStr: '${idStr}', flag: ${flag}})"
  }

  def getCypherCreateRel(fromIdStr: Long, toIdStr: Long, labels: Array[String] = Array()): String = {
    var labelStr = new StringBuilder()
    labels.foreach(l => {labelStr ++= s":${l}"})
    s"match (f), (t) where f.idStr='${fromIdStr}' AND t.idStr='${toIdStr}' CREATE (f)-[rel${labelStr}]->(t)"
  }

  def getCypherQueryByAttrEqual(equalString: String, limit: Int = 0): String = {
    val limitStr = limit match {
      case 0 => ""
      case _ => s" limit ${limit}"
    }
    s"match (n) where n.${equalString} return n${limitStr}"
  }

  def getCypherQueryByAttrGtLt(gtString: String, limit: Int = 0): String = {
    val limitStr = limit match {
      case 0 => ""
      case _ => s" limit ${limit}"
    }
    s"match (n) where n.${gtString} return n${limitStr}"
  }

  def getCypherQueryByRelFrom(fromId_p: Long, labels: Array[String] = Array()): String = {
    var labelStr = new StringBuilder()
    labels.foreach(l => {labelStr ++= s":${l}"})
    s"match (f)-[r${labelStr}]->(t) where f.id_p=${fromId_p} return count(t)"
  }

  def genBatchCreation(startId: Long, endId: Long): Array[String] = {
    val resArrayNode: mutable.ArrayBuffer[String] = new mutable.ArrayBuffer[String]()
    val resArrayRel: mutable.ArrayBuffer[String] = new mutable.ArrayBuffer[String]()
    for(id <- startId until endId){
      val labels =  Array(s"label${id%10}")
      resArrayNode += getCypherCreateNode(id, s"${id}", id%2==0, labels)
      if (id!=startId){
        resArrayRel += getCypherCreateRel(id, Random.nextInt(id.toInt), labels)
      }
    }
    (resArrayNode++resArrayRel).toArray
  }

  def genBatchQuery(randomUntil: Int): Array[String] = {
    val resArray: mutable.ArrayBuffer[String] = new mutable.ArrayBuffer[String]()
    val id = Random.nextInt(randomUntil)
    resArray+=getCypherQueryByAttrEqual(s"id_p=${id}", 1)
    resArray+=getCypherQueryByAttrEqual(s"id_p=${id}")
    resArray+=getCypherQueryByAttrGtLt(s"id_p<${id}", 1)
    resArray+=getCypherQueryByAttrGtLt(s"id_p<${id}", 10)
    resArray+=getCypherQueryByAttrGtLt(s"id_p<${id}")
    resArray+=getCypherQueryByRelFrom(id)
    resArray+=getCypherQueryByRelFrom(id, Array(s"label${id%10}"))
    resArray.toArray
  }

  def demoQuerys(range: Int = 10): Array[String] = {
    genBatchCreation(1, range) ++ genBatchQuery(range)
  }

  @Test
  def showDemoQuerys(): Unit = {
    println(demoQuerys().mkString("\n"))
  }

  @Test
  def creatTest(): Unit = {
    val q = demoQuerys().head
  }
}