package cn.pandadb.kernel.distribute.meta

import java.util.concurrent.atomic.AtomicInteger
import cn.pandadb.kernel.distribute.index.PandaDistributedIndexStore
import scala.collection.mutable

trait DistributedNameStore {
  val initInt: Int
  val indexStore: PandaDistributedIndexStore
  val indexName: String

  var idGenerator: AtomicInteger = new AtomicInteger(initInt)
  var mapString2Int: mutable.Map[String, Int] = mutable.Map[String, Int]()
  var mapInt2String: mutable.Map[Int, String] = mutable.Map[Int, String]()

  private def addToDB(labelName: String): Int = {
    val id = idGenerator.incrementAndGet()
    mapString2Int += labelName -> id
    mapInt2String += id -> labelName
    indexStore.addDoc(indexName, labelName, id)
    id
  }

  def key(id: Int): Option[String] = mapInt2String.get(id)

  def id(labelName: String): Option[Int] = mapString2Int.get(labelName)

  def getOrAddId(labelName: String): Int =
    id(labelName).getOrElse(addToDB(labelName))

  def ids(keys: Set[String]): Set[Int] = {
    val newIds = keys.map {
      key =>
        val opt = mapString2Int.get(key)
        if (opt.isDefined) {
          opt.get
        }
        else {
          addToDB(key)
        }
    }
    newIds
  }

  def delete(labelName: String): Unit = {
    val id = mapString2Int(labelName)
    mapString2Int -= labelName
    mapInt2String -= id
    indexStore.deleteDoc(indexName, id)
  }

  def loadAll(): Unit = {
    if (!indexStore.indexIsExist(indexName)) indexStore.createIndex(indexName)
    idGenerator = new AtomicInteger(initInt)

    val data = indexStore.loadAllMeta(indexName)
    mapString2Int = mutable.Map(data._1.toSeq:_*)
    mapInt2String = mutable.Map(data._2.toSeq:_*)

    val maxId: Int = {
      val tmpId = {
        if (mapInt2String.nonEmpty) mapInt2String.keys.toList.maxBy(f => f)
        else 0
      }
      math.max(initInt, tmpId)
    }
    idGenerator.set(maxId)
  }
}
