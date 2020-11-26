package cn.pandadb.kernel.direct

import scala.collection.mutable.HashMap
import io.netty.buffer.{ByteBuf, Unpooled}
import cn.pandadb.kernel.direct.{BlockId => EndNodesBlockId}

import scala.collection.mutable

case class RelationshipRecord(fromNode: Long, endNode: Long, label: Int)

class RelationshipHits(fromNode: Long, endNodes: Iterator[Long], label: Int) extends Iterator[RelationshipRecord] {
  override def hasNext: Boolean = endNodes.hasNext

  override def next(): RelationshipRecord = {
    RelationshipRecord(fromNode, endNodes.next(), label)
  }
}

object LongByteBuf {
  val RECORD_SIZE: Int = 8 // long need 8 Bytes

  def allocate(recordsCount: Int): ByteBuf = {
    Unpooled.directBuffer(recordsCount*RECORD_SIZE,recordsCount*RECORD_SIZE)
  }

  def recordPostionToBufIndex(position: Int): Int = position * RECORD_SIZE

  def checkByteBufIndex(byteBuf: ByteBuf, index: Int): Boolean = {
    if (index < 0 || index % RECORD_SIZE != 0 || index > byteBuf.capacity() - RECORD_SIZE ) {
      throw new Exception("index is invalid ")
    }
    true
  }

  def setAllAs(byteBuf: ByteBuf, value: Long): Unit = {
    if(value == 0) {
      byteBuf.setZero(0,byteBuf.capacity())
    }
    else {
      var index: Int = 0
      while (index < byteBuf.capacity()) {
        byteBuf.setLong(index, value)
        index += RECORD_SIZE
      }
    }
  }

  def set(byteBuf: ByteBuf, position: Int, value: Long) = {
    val bufIndex = recordPostionToBufIndex(position)
    checkByteBufIndex(byteBuf, bufIndex)
    byteBuf.setLong(bufIndex, value)
  }

  def get(byteBuf: ByteBuf, position: Int): Long = {
    val bufIndex = recordPostionToBufIndex(position)
    checkByteBufIndex(byteBuf, bufIndex)
    byteBuf.getLong(bufIndex)
  }
}

class LongByteBuf(recordsCount: Int, initRecordValue:Long = 0) {

  private val byteBuf = LongByteBuf.allocate(recordsCount)
  LongByteBuf.setAllAs(byteBuf, initRecordValue) // set n

  def get(recordPosition: Int): Long = {
    LongByteBuf.get(byteBuf, recordPosition)
  }

  def set(recordPosition: Int, value: Long): Unit = {
    LongByteBuf.set(byteBuf, recordPosition, value)
  }

  def release(): Boolean = {
    byteBuf.release()
  }
}

class OutEdgeRelationIndex(recordsCountPerBlock:Int = 1000*10000) {
  private val RECORDS_COUNT_PER_BLOCK: Int = recordsCountPerBlock
  private val INVALID_RECORD_VALUE: Long = 0L

  private val fromNodesGroupMap = HashMap[String, LongByteBuf]()

  private def formatAsNodesGroupMapKey(fromNode: Long, label: Int): String = {
    (new mutable.StringBuilder().append(label).append("_").append(fromNode/RECORDS_COUNT_PER_BLOCK)).toString
  }

  private def formatAsPostionInNodesGroup(fromNode: Long): Int = {
    (fromNode % RECORDS_COUNT_PER_BLOCK).toInt
  }

  private def newFromNodesGroup(): LongByteBuf = {
    new LongByteBuf(RECORDS_COUNT_PER_BLOCK, INVALID_RECORD_VALUE)
  }

  // use one long save EndNodesBlockId: pageId->high32, offset->low32
  private def endNodesBlockIdToLong(id: EndNodesBlockId): Long = {
    var low: Long = id.offset.toLong
    var high: Long = id.pageId.toLong
    (low& 0xFFFFFFFFL) | ((high << 32) & 0xFFFFFFFF00000000L)
  }

  private def endNodesBlockIdFromLong(num: Long): EndNodesBlockId = {
    val offset: Int = (num & 0xFFFFFFFFL).toInt
    val pageId: Short = ((num & 0xFFFFFFFF00000000L) >>32).toShort
    EndNodesBlockId(pageId, offset)
  }

  private def operateOutEdge(fromNode:Long, toNode:Long, label:Int, operator:String): Boolean = {
    val groupKey = formatAsNodesGroupMapKey(fromNode, label)
    val group: LongByteBuf = fromNodesGroupMap.getOrElseUpdate(groupKey, newFromNodesGroup())
    val record: Long = group.get(formatAsPostionInNodesGroup(fromNode))
    val endNodesBlockId = endNodesBlockIdFromLong(record)

    // todo: It's a bit wasteful to create new objects every time, it should be improved
    val endNodesBlockManager = new OutGoingEdgeBlockManager(endNodesBlockId)
    var operateResult = true
    operator match {
      case "add" => endNodesBlockManager.put(toNode)
      case "delete" => endNodesBlockManager.delete(toNode)
    }

    if (endNodesBlockId != endNodesBlockManager.getBeginBlockId) {
      group.set(formatAsPostionInNodesGroup(fromNode), endNodesBlockIdToLong(endNodesBlockManager.getBeginBlockId))
    }

    operateResult
  }

  def getOutEdges(fromNode:Long, label: Int): RelationshipHits = {
    assert(fromNode>0 && label>0)
    val groupOption = fromNodesGroupMap.get(formatAsNodesGroupMapKey(fromNode, label))
    var record: Long = INVALID_RECORD_VALUE
    if (groupOption.isDefined) {
      val group = groupOption.get
      record = group.get(formatAsPostionInNodesGroup(fromNode))
    }
    val endNodesBlockId: EndNodesBlockId = endNodesBlockIdFromLong(record)
    val endNodesBlockManager = new OutGoingEdgeBlockManager(endNodesBlockId)
    new RelationshipHits(fromNode, endNodesBlockManager.getAllBlockNodeIds(), label)

  }

  def addRelationship(fromNode:Long, toNode:Long, label:Int): Unit = {
    assert(fromNode>0 && toNode>0 && label>0)
    operateOutEdge(fromNode, toNode, label, "add")
  }

  def deleteRelation(fromNode:Long, toNode:Long, label:Int): Unit = {
    assert(fromNode>0 && toNode>0 && label>0)
    operateOutEdge(fromNode, toNode, label, "delete")
  }

}

object StoreTest {
  def main(args: Array[String]): Unit = {
    // args: 0-PerMapRecordSize、 1-fromNodesCountPerLabel、2-relationLabelCount、3-outEdgeCountPerNode
    var mapRecordSize = 1000
    var fromNodesCount = 10000
    var relTypeCount = 100
    var outEdgeCountPerNode = 1000
    if (args.length>=1) mapRecordSize = args(0).toInt
    if (args.length>=2) fromNodesCount = args(1).toInt
    if (args.length>=3) relTypeCount = args(3).toInt
    if (args.length>=4) outEdgeCountPerNode = args(0).toInt

    println(s"all relationship count: ${relTypeCount*fromNodesCount*outEdgeCountPerNode}")

    val store = new OutEdgeRelationIndex(mapRecordSize)

    val beginTime = System.currentTimeMillis()

    for (relLabel <- 1 to relTypeCount) {
      for (fromNode <- 1L to fromNodesCount.toLong) {
        for (toNode <- 1L to outEdgeCountPerNode.toLong) {
          store.addRelationship(fromNode, toNode, relLabel)
        }
        if (fromNode%1000 == 0) println(s"writed fromNode: ${relLabel}-${fromNode}")
      }

      println(s"writed relationLabel: ${relLabel}")
      println(s"used time (ms): ${System.currentTimeMillis()-beginTime}")

    }

    val endTime = System.currentTimeMillis()
    println(s"used time (ms): ${endTime-beginTime}")
  }
}
