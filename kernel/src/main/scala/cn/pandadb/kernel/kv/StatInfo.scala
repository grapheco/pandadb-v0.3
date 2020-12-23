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

    //KeyHandler

    ByteUtils.stringFromBytes(rocksDB.get(ByteUtils.longToBytes(0))).split(",").foreach(nodeLabelSchema += _)
    ByteUtils.stringFromBytes(rocksDB.get(ByteUtils.longToBytes(1))).split(",").foreach(relLabelSchema += _)
    ByteUtils.stringFromBytes(rocksDB.get(ByteUtils.longToBytes(2))).split(",").foreach(indexPropertySchema += _)
    //rocksDB.get(ByteUtils.longToBytes(0)).toString.split(",").foreach(nodeLabelSchema += _)
    //rocksDB.get(ByteUtils.longToBytes(1)).toString.split(",").foreach(relLabelSchema += _)
    //rocksDB.get(ByteUtils.longToBytes(2)).toString.split(",").foreach(indexPropertySchema += _)

    //nodeLabelSchema.foreach(u => labelCnt += u->rocksDB.get(u.getBytes).toString.toLong)
    nodeLabelSchema.foreach(u => labelCnt += u->ByteUtils.getLong(rocksDB.get(u.getBytes), 0))
    relLabelSchema.foreach(u => relLabelCnt += u->ByteUtils.getLong(rocksDB.get(u.getBytes), 0))
    indexPropertySchema.foreach(u => propertyIndexCnt += u->ByteUtils.getLong(rocksDB.get(u.getBytes), 0))

    allNodesCnt = ByteUtils.getLong(rocksDB.get(ByteUtils.longToBytes(3)), 0)
    allRelCnt = ByteUtils.getLong(rocksDB.get(ByteUtils.longToBytes(4)), 0)



  }


  def save(): Unit = {
    rocksDB.put(ByteUtils.longToBytes(0), ByteUtils.stringToBytes(nodeLabelSchema.mkString(",")))
    rocksDB.put(ByteUtils.longToBytes(1), ByteUtils.stringToBytes(relLabelSchema.mkString(",")))
    rocksDB.put(ByteUtils.longToBytes(2), ByteUtils.stringToBytes(indexPropertySchema.mkString(",")))
    rocksDB.put(ByteUtils.longToBytes(3), ByteUtils.longToBytes(allNodesCnt))
    rocksDB.put(ByteUtils.longToBytes(4), ByteUtils.longToBytes(allRelCnt))

   /* rocksDB.put(ByteUtils.longToBytes(1), relLabelSchema.mkString(",").getBytes)
    rocksDB.put(ByteUtils.longToBytes(2), indexPropertySchema.mkString(",").getBytes)
    rocksDB.put(ByteUtils.longToBytes(3), allNodesCnt.toString.getBytes)
    rocksDB.put(ByteUtils.longToBytes(4), allRelCnt.toString.getBytes)*/


    labelCnt.foreach(u => rocksDB.put(ByteUtils.stringToBytes(u._1), ByteUtils.longToBytes(u._2)))
    relLabelCnt.foreach(u => rocksDB.put(ByteUtils.stringToBytes(u._1), ByteUtils.longToBytes(u._2)))
    propertyIndexCnt.foreach(u => rocksDB.put(ByteUtils.stringToBytes(u._1), ByteUtils.longToBytes(u._2)))

  }


  def getAllNodesCnt(): Long = {
    allNodesCnt
  }

  def addNodes(cnt: Long): Unit ={
    allNodesCnt += cnt
  }

  def addRels(cnt: Long): Unit ={
    allRelCnt += cnt
  }

  def getAllRelCnt(): Long = {
    allRelCnt
  }

  def delNodes(cnt: Long): Unit ={
    allNodesCnt -= cnt
  }


  def delRels(cnt: Long): Unit ={
    allRelCnt -= cnt
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
    addNodes(cnt)
  }

  def delNodeLabelCnt(label: String, cnt: Long): Unit = {
    if (nodeLabelSchema.contains(label)){
      labelCnt += label -> (labelCnt.get(label).get - cnt)
    }
    delNodes(cnt)
  }

  def addRelLabelCnt(label: String, cnt: Long): Unit = {
    if (relLabelSchema.contains(label)){
      relLabelCnt += label -> (relLabelCnt.get(label).get + cnt)
    }
    else {
      relLabelSchema += label
      relLabelCnt += label -> cnt
    }

    addRels(cnt)

  }

  def delRelLabelCnt(label: String, cnt: Long): Unit = {
    if (relLabelSchema.contains(label)){
      relLabelCnt += label -> (relLabelCnt.get(label).get - cnt)
    }
    delRels(cnt)
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
    if (indexPropertySchema.contains(label+property)){
      propertyIndexCnt += (label+property)->(propertyIndexCnt.get(label+property).get - cnt)
    }
  }

}
