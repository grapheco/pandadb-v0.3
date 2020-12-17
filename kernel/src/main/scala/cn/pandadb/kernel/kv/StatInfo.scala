package cn.pandadb.kernel.kv

import org.rocksdb.RocksDB

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer



class StatInfo(rocksDB: RocksDB) {

  /*
   0->label: student, person...
   1->rellabel: fanOf, knows...
   2->indexProperty: student+age, person+name
   3->allNodesCnt
   4->allRelCnt
     ....
     ....
  */
  //private val labelStr = "label"
  var nodeLabelSchema: ArrayBuffer[String] = ArrayBuffer[String]()
  var relLabelSchema: ArrayBuffer[String] = ArrayBuffer[String]()
  var indexPropertySchema: ArrayBuffer[String] = ArrayBuffer[String]()


  var labelCnt: mutable.Map[String, Long] = mutable.Map[String, Long]()
  var relLabelCnt: mutable.Map[String, Long] = mutable.Map[String, Long]()
  var allNodesCnt: Long = -1
  var allRelCnt: Long = -1
  var propertyIndexCnt: mutable.Map[String, Long] = mutable.Map[String, Long]()

  //labelCnt += "student" -> 1
  //relLabelCnt += "knows" -> 1
 // propertyIndexCnt += ("student" -> "name") -> 1

  def start(): Unit = {

    rocksDB.get(ByteUtils.longToBytes(0)).toString.split(",").foreach(nodeLabelSchema += _)
    rocksDB.get(ByteUtils.longToBytes(1)).toString.split(",").foreach(relLabelSchema += _)
    rocksDB.get(ByteUtils.longToBytes(2)).toString.split(",").foreach(indexPropertySchema += _)

    nodeLabelSchema.foreach(u => labelCnt += u->rocksDB.get(u.getBytes).toString.toLong)
    relLabelSchema.foreach(u => relLabelCnt += u->rocksDB.get(u.getBytes).toString.toLong)
    indexPropertySchema.foreach(u => propertyIndexCnt += u->rocksDB.get(u.getBytes).toString.toLong)

    allNodesCnt = rocksDB.get(ByteUtils.longToBytes(3)).toString.toLong
    allRelCnt = rocksDB.get(ByteUtils.longToBytes(4)).toString.toLong



  }


  def save(): Unit = {
    rocksDB.put(ByteUtils.longToBytes(0), nodeLabelSchema.mkString(",").getBytes)
    rocksDB.put(ByteUtils.longToBytes(1), relLabelSchema.mkString(",").getBytes)
    rocksDB.put(ByteUtils.longToBytes(2), indexPropertySchema.mkString(",").getBytes)
    rocksDB.put(ByteUtils.longToBytes(3), allNodesCnt.toString.getBytes)
    rocksDB.put(ByteUtils.longToBytes(4), allRelCnt.toString.getBytes)

    labelCnt.foreach(u => rocksDB.put(u._1.getBytes, u._2.toString.getBytes))
    relLabelCnt.foreach(u => rocksDB.put(u._1.getBytes, u._2.toString.getBytes))
    propertyIndexCnt.foreach(u => rocksDB.put(u._1.getBytes, u._2.toString.getBytes))

  }


  def getAllNodesCnt(): Long = {
    allNodesCnt
  }

  def getAllRelCnt(): Long = {
    allRelCnt
  }

  def getLabelCnt(label: String): Option[Long] = {
    labelCnt.get(label)
  }

  def getRelLabelCnt(label: String): Option[Long] = {
    relLabelCnt.get(label)
  }

  def getPropertyIndexCnt(label: String, property: String): Option[Long] = {
    propertyIndexCnt.get(label+property)
  }

  def addNodeLabelCnt(label: String, cnt: Long): Unit = {
    if (nodeLabelSchema.contains(label)){
      labelCnt += label -> (labelCnt.get(label).get + cnt)
    }
    else {
      nodeLabelSchema += label
      labelCnt += label -> cnt
    }
    allNodesCnt += cnt
  }

  def delNodeLabelCnt(label: String, cnt: Long): Unit = {
    if (nodeLabelSchema.contains(label)){
      labelCnt += label -> (labelCnt.get(label).get - cnt)
    }
    allNodesCnt -= cnt
  }

  def addRelLabelCnt(label: String, cnt: Long): Unit = {
    if (relLabelSchema.contains(label)){
      relLabelCnt += label -> (relLabelCnt.get(label).get + cnt)
    }
    else {
      relLabelSchema += label
      relLabelCnt += label -> cnt
    }

    allRelCnt += cnt

  }

  def delRelLabelCnt(label: String, cnt: Long): Unit = {
    if (relLabelSchema.contains(label)){
      relLabelCnt += label -> (relLabelCnt.get(label).get - cnt)
    }
    allRelCnt -= cnt
  }

  def addPropertyIndexCnt(label: String, property: String, cnt : Long): Unit = {
    if(indexPropertySchema.contains(label+property)) {
      propertyIndexCnt += (label+property)->(propertyIndexCnt.get(label+property).get + cnt)
    }
    else {
      indexPropertySchema += label+property
      propertyIndexCnt += (label+property)->cnt
    }
  }

  def delPropertyIndexCnt(label: String, property: String, cnt : Long): Unit = {
    if (relLabelSchema.contains(label)){
      propertyIndexCnt += (label+property)->(propertyIndexCnt.get(label+property).get - cnt)
    }
  }
  //def setLabelCnt
  //def addNodes()

}
