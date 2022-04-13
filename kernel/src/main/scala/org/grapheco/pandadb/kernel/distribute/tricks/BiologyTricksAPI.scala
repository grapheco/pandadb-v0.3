package org.grapheco.pandadb.kernel.distribute.tricks

import org.grapheco.lynx.{LynxNodeLabel, LynxPropertyKey, LynxValue, NodeFilter}
import org.grapheco.pandadb.kernel.distribute.relationship.RelationDirection
import org.grapheco.pandadb.kernel.distribute.{DistributedGraphFacade, DistributedKeyConverter}
import org.grapheco.pandadb.kernel.kv.ByteUtils
import org.grapheco.pandadb.kernel.store.{PandaNode, PandaRelationship}
import org.tikv.common.util.ScanOption
import org.tikv.shade.com.google.protobuf.ByteString

import java.util.regex.Pattern
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-04-12 10:24
 */
class BiologyTricksAPI(api: DistributedGraphFacade) {


  def basicInfoOfTaxonomy(taxId: String, schema: Seq[String]): BioDataFrame = {
    val node = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId))))

    BioDataFrame(schema, node.map(f => Seq(f)))
  }

  def totalPubmedOfTaxonomy(taxId: String, schema: Seq[String]): BioDataFrame = {
    val node = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val res = api.countOutRelations(node.id.value, api.getRelationTypeId("taxonomy2pubmed").get)
    BioDataFrame(schema, Iterator(Seq(res)))
  }

  def countCited(taxId: String, schema: Seq[String]): BioDataFrame = {
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val endNodeIds = api.findOutRelationsEndNodeIds(startNode.id.value, api.getRelationTypeId("taxonomy2pubmed").get)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, api.getNodeLabelId("pubmed").get))
    val count = resNodes
      .map(n => {
        val res = n.props(LynxPropertyKey("cited_num")).value.toString
        if (res.nonEmpty) res.toInt
        else 0
      }).sum
    BioDataFrame(schema, Iterator(Seq(count)))
  }

  def count3YearCited(taxId: String, year: Int, schema: Seq[String]): BioDataFrame = {
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val endNodeIds = api.findOutRelationsEndNodeIds(startNode.id.value, api.getRelationTypeId("taxonomy2pubmed").get)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, api.getNodeLabelId("pubmed").get))
    val count = resNodes.count(n => n.props(LynxPropertyKey("publish_date")).value.toString.slice(0, 4).toInt > year)
    BioDataFrame(schema, Iterator(Seq(count)))
  }

  def earliest(taxId: String, schema: Seq[String]): BioDataFrame = {
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val endNodeIds = api.findOutRelationsEndNodeIds(startNode.id.value, api.getRelationTypeId("taxonomy2pubmed").get)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, api.getNodeLabelId("pubmed").get))
    val year = resNodes.map(n => n.props(LynxPropertyKey("publish_date")).value.toString.slice(0, 4).toInt).min
    BioDataFrame(schema, Iterator(Seq(year)))
  }

  def findTop3LevelParent(taxId: String, schema: Seq[String]): BioDataFrame = {
    val typeId = api.getRelationTypeId("parent")
    val node1 = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()

    val path1 = outPattern(Seq(node1.id.value), typeId, "taxonomy")
    val path2 = outPattern(path1.map(f => f._2.id.value), typeId, "taxonomy")
    val path3 = outPattern(path2.map(f => f._2.id.value), typeId, "taxonomy")

    val data1 = path1.flatMap(f => Seq(node1, f._1, f._2))
    val data2 = path2.flatMap(f => data1 ++ Seq(f._1, f._2))
    val data3 = path3.flatMap(f => data2 ++ Seq(f._1, f._2))
    BioDataFrame(schema, Iterator(data1, data2, data3).map(f => Seq(f)))
  }

  private def outPattern(startNodeIds: Seq[Long], relTypeId: Option[Int], endNodesLabel: String): Seq[(PandaRelationship, PandaNode)] = {
    val pattern = startNodeIds.flatMap(leftId => {
      val r = api.findOutRelations(leftId, relTypeId)
      r.map(rr => (rr, api.getNodeById(rr.endNodeId.value, endNodesLabel).get))
    })
    pattern
  }

  def projectInfo(taxId: String, limit: Int, schema: Seq[String]): BioDataFrame = {
    val pattern = Pattern.compile(".*/.*")
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val endNodeIds = api.findOutRelationsEndNodeIds(startNode.id.value, api.getRelationTypeId("taxonomy2bioproject").get)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, api.getNodeLabelId("bioproject").get))

    val res = resNodes.toSeq.map(node => (node.props(LynxPropertyKey("smdt")).value.toString, node)).sortBy(f => f._1).reverse

    val dataArray: ArrayBuffer[PandaNode] = ArrayBuffer.empty
    val dataLength = res.length
    var index = 0
    while (dataArray.length <= limit && (index < dataLength)) {
      if (pattern.matcher(res(index)._1).matches()) dataArray.append(res(index)._2)
      index += 1
    }
    val r = dataArray.map(n =>
      Seq(n.props(LynxPropertyKey("title")).value.toString, n.props(LynxPropertyKey("bioproject_id")).value.toString))

    BioDataFrame(schema, r.toIterator)
  }

  def geneOfTaxonomy(taxId: String, limit: Int, schema: Seq[String]): BioDataFrame = {
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val endNodeIds = api.findOutRelations(startNode.id.value, api.getRelationTypeId("taxonomy2gene")).map(f => f.endNodeId.value)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, api.getNodeLabelId("gene").get))
    val res = resNodes.slice(0, limit).map(f => Seq(f.props(LynxPropertyKey("title")).value, f.props(LynxPropertyKey("gene_id")).value))
    BioDataFrame(schema, res)
  }

  def genomeOfTaxonomy(taxId: String, limit: Int, schema: Seq[String]): BioDataFrame = {
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val endNodeIds = api.findOutRelations(startNode.id.value, api.getRelationTypeId("taxonomy2genome")).map(f => f.endNodeId.value)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, api.getNodeLabelId("genome").get))
    val res = resNodes.slice(0, limit).map(f => Seq(f.props(LynxPropertyKey("genome_id")).value, f.props(LynxPropertyKey("aacc")).value))
    BioDataFrame(schema, res)
  }

  def paperTendency(taxId: String, schema: Seq[String]): BioDataFrame = {
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val endNodeIds = api.findOutRelations(startNode.id.value, api.getRelationTypeId("taxonomy2pubmed")).map(f => f.endNodeId.value)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, api.getNodeLabelId("pubmed").get))
    val yearCountMap = mutable.Map[Int, Int]()
    resNodes.foreach(node => {
      val year = node.props(LynxPropertyKey("publish_date")).value.toString.slice(0, 4).toInt
      if (year >= 1980) {
        if (yearCountMap.contains(year)) {
          yearCountMap(year) = yearCountMap(year) + 1
        }
        else yearCountMap += year -> 1
      }
    })
    val r = yearCountMap.toSeq.sortBy(f => f._1)
    BioDataFrame(schema, r.toIterator.map(f => Seq(f._1, f._2)))
  }

  def topKTendency(taxId: String, startYear: Int, endYear: Int, limit: Int, schema: Seq[String]): BioDataFrame = {
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val endNodeIds = api.findOutRelations(startNode.id.value, api.getRelationTypeId("taxonomy2pubmed")).map(f => f.endNodeId.value)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, api.getNodeLabelId("pubmed").get))

    val betweenYearData = resNodes.filter(p => {
      p.props(LynxPropertyKey("keywords")).value.toString.nonEmpty && {
        val year = p.props(LynxPropertyKey("publish_date")).value.toString.slice(0, 4).toInt
        year >= startYear && year <= endYear
      }
    }).duplicate

    val keywords = betweenYearData._1.flatMap(p => p.props(LynxPropertyKey("keywords")).value.toString.split(";").distinct)
      .toSeq
      .groupBy(k => k)
      .map(f => (f._1, f._2.length)).toSeq
      .sortBy(f => f._2).reverse.slice(0, limit).map(f => (f._1.toLowerCase, f._2)).toMap

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
    BioDataFrame(schema, res.toIterator.map(f => Seq(f._1._1, f._1._2, f._2)))
  }

  def keywordRelation(taxId: String, limit: Int, schema: Seq[String]): BioDataFrame = {
    val key = LynxPropertyKey("keywords")
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val endNodeIds = api.findOutRelations(startNode.id.value, api.getRelationTypeId("taxonomy2pubmed")).map(f => f.endNodeId.value)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, api.getNodeLabelId("pubmed").get))
    val keywords = resNodes.filter(p => p.props(key).value.toString.nonEmpty).map(p => p.props(key).value.toString.trim.toLowerCase().split(";")).toSeq
    val leftRight = keywords.map(line => {
      val length = line.length
      val left = line.flatMap(str => {
        val leftArr = ArrayBuffer[String]()
        var count = 0
        while (count < length) {
          leftArr.append(str)
          count += 1
        }
        leftArr
      }).toSeq
      val right = {
        var rightArr: Seq[String] = Seq.empty
        var count = 0
        while (count < length) {
          rightArr = rightArr ++ line
          count += 1
        }
        rightArr
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
      if (queue.length > limit) queue.dequeue()
    })
    val r = queue.toArray.sortBy(f => f._2).reverse
    BioDataFrame(schema, r.toIterator.map(f => Seq(f._1, f._2)))
  }

  def countKey(taxId: String, limit: Int, schema: Seq[String]): BioDataFrame = {
    val key = LynxPropertyKey("keywords")
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val endNodeIds = api.findOutRelations(startNode.id.value, api.getRelationTypeId("taxonomy2pubmed")).map(f => f.endNodeId.value)
    val resNodes = endNodeIds.grouped(1000).flatMap(group => api.getNodesByIds(group, api.getNodeLabelId("pubmed").get))

    val keywordsArr = ArrayBuffer[String]()

    resNodes.foreach(node => {
      if (node.props(key).value.toString.nonEmpty) {
        val line = node.props(key).value.toString.trim.toLowerCase.split(";")
        keywordsArr.append(line: _*)
      }
    })

    val a = keywordsArr.groupBy(f => f).map(f => (f._1, f._2.length)).toSeq.sortBy(f => f._2).reverse.slice(0, limit)
    BioDataFrame(schema, a.toIterator.map(f => Seq(f._1, f._2)))
  }

  def distributionOfCountryOfPaper(taxId: String, schema: Seq[String]): BioDataFrame = {
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val relType = api.getRelationTypeId("taxonomy2pubmed")
    val relType2 = api.getRelationTypeId("pubmed2country")
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
    BioDataFrame(schema, count.toIterator.map(f => Seq(f._1, f._2)))
  }

  def distributionOfCountryOfProject(taxId: String, schema: Seq[String]): BioDataFrame = {
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val relType = api.getRelationTypeId("taxonomy2bioproject")
    val relType2 = api.getRelationTypeId("bioproject2country")
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
    val endNode2 = res.map(kv => ByteUtils.getLong(kv.getKey.toByteArray, 13))
    val country = endNode2.grouped(1000).flatMap(f => api.getNodesByIds(f, labelType)).map(n => n.props(LynxPropertyKey("country")).value.toString)
    val count = country.toSeq.groupBy(f => f).map(f => (f._1, f._2.length))
    BioDataFrame(schema, count.toIterator.map(f => Seq(f._1, f._2)))
  }

  def relativePaperCount(taxId: String, schema: Seq[String]): BioDataFrame = {
    totalPubmedOfTaxonomy(taxId, schema)
  }

  def relativePaper(taxId: String, skip: Int, limit: Int, schema: Seq[String]): BioDataFrame = {
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
    val relType = api.getRelationTypeId("taxonomy2pubmed")
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
    res.drop(skip)
    BioDataFrame(schema, res.slice(0, limit).map(f => Seq(f._1, f._2, f._3, f._4, f._5)))
  }

  def relativePNG(taxId: String, schema: Seq[String]): BioDataFrame = {

    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
    val relType = api.getRelationTypeId("taxonomy2pubmed")
    val relType2 = api.getRelationTypeId("pubmed2pubmed_pdf_png")
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
    BioDataFrame(schema, pubmed_pdf_png.map(f => Seq(f._1, f._2, f._3)))
  }

  def countProject(taxId: String, schema: Seq[String]): BioDataFrame = {
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue("9606")))).next()
    val relType = api.getRelationTypeId("taxonomy2bioproject")
    val rels = api.countOutRelations(startNode.id.value, relType.get)
    BioDataFrame(schema, Iterator(Seq(rels)))
  }

  def relativeProject(taxId: String, skip: Int, limit: Int, schema: Seq[String]): BioDataFrame = {
    val startNode = api.getNodesByIndex(NodeFilter(Seq(LynxNodeLabel("taxonomy")), Map(LynxPropertyKey("tax_id") -> LynxValue(taxId)))).next()
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
    res.drop(skip)
    BioDataFrame(schema, res.slice(0, limit).map(f => Seq(f._1, f._2, f._3, f._4)))
  }
}
