package cn.pandadb.tools.importer


import java.util.concurrent.{ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicLong

import cn.pandadb.kernel.distribute.DistributedKVAPI

import scala.collection.convert.ImplicitConversions._

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 15:07 2021/1/20
 * @Modified By:
 */
case class GlobalArgs(coreNum: Int = Runtime.getRuntime().availableProcessors(),
                      importerStatics : ImporterStatics,
                      estNodeCount: Long, estRelCount: Long,
                      nodeDB: DistributedKVAPI, nodeLabelDB: DistributedKVAPI,
                      relationDB: DistributedKVAPI,inRelationDB: DistributedKVAPI,
                      outRelationDB: DistributedKVAPI,relationTypeDB: DistributedKVAPI, metaDB: DistributedKVAPI)

case class ImporterStatics() {
  private val globalNodeCount: AtomicLong = new AtomicLong(0)
  private val globalRelCount: AtomicLong = new AtomicLong(0)
  private val globalNodePropCount: AtomicLong = new AtomicLong(0)
  private val globalRelPropCount: AtomicLong = new AtomicLong(0)

  private val nodeCountByLabel: ConcurrentHashMap[Int, Long] = new ConcurrentHashMap[Int, Long]()
  private val relCountByType: ConcurrentHashMap[Int, Long] = new ConcurrentHashMap[Int, Long]()

  def getGlobalNodeCount = globalNodeCount
  def getGlobalRelCount = globalRelCount
  def getGlobalNodePropCount = globalNodePropCount
  def getGlobalRelPropCount = globalRelPropCount

  def getNodeCountByLabel: Map[Int, Long] = nodeCountByLabel.toMap

  def getRelCountByType: Map[Int, Long] = relCountByType.toMap

  def nodeCountAddBy(count: Long): Long = globalNodeCount.addAndGet(count)

  def relCountAddBy(count: Long): Long = globalRelCount.addAndGet(count)

  def nodePropCountAddBy(count: Long): Long = globalNodePropCount.addAndGet(count)

  def relPropCountAddBy(count: Long): Long = globalRelPropCount.addAndGet(count)

  def nodeLabelCountAdd(label: Int, count: Long): Long = this.synchronized {
    if(nodeCountByLabel.contains(label)){
      val countBeforeAdd = nodeCountByLabel.get(label)
      val countAfterAdd = countBeforeAdd + count
      nodeCountByLabel.put(label, countAfterAdd)
      countAfterAdd
    } else {
      nodeCountByLabel.put(label, count)
      count
    }
  }

  def relTypeCountAdd(typeId: Int, count: Long): Long = this.synchronized {
    if(relCountByType.contains(typeId)){
      val countBeforeAdd = relCountByType.get(typeId)
      val countAfterAdd = countBeforeAdd + count
      relCountByType.put(typeId, countAfterAdd)
      countAfterAdd
    } else {
      relCountByType.put(typeId, count)
      count
    }
  }
}