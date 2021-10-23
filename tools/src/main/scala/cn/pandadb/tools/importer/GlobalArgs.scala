package cn.pandadb.tools.importer

import cn.pandadb.kernel.kv.db.KeyValueDB
import cn.pandadb.kernel.kv.meta.Statistics

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 15:07 2021/1/20
 * @Modified By:
 */
case class GlobalArgs(coreNum: Int = Runtime.getRuntime().availableProcessors(),
                      importerStatics : ImporterStatics,
                      estNodeCount: Long, estRelCount: Long,
                      nodeDB: KeyValueDB, nodeLabelDB: KeyValueDB,
                      relationDB: KeyValueDB, inrelationDB: KeyValueDB, outRelationDB: KeyValueDB, relationTypeDB: KeyValueDB, statistics: Statistics)

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
      nodeCountByLabel.put(typeId, count)
      count
    }
  }
}