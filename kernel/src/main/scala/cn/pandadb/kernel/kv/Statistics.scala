package cn.pandadb.kernel.kv

import org.rocksdb.RocksDB

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Statistics(rocksDB: RocksDB) {

  /*
   0->label: student, person...
   1->rellabel: fanOf, knows...
   2->indexProperty: student+age, person+name
   3->allNodesCount
   4->allRelCount
     ....
     ....
  */
  //private val labelStr = "label"
  var nodeLabelSchema: ArrayBuffer[String] = ArrayBuffer[String]()
  var relLabelSchema: ArrayBuffer[String] = ArrayBuffer[String]()
  var indexPropertySchema: ArrayBuffer[String] = ArrayBuffer[String]()


  var labelCount: mutable.Map[String, Long] = mutable.Map[String, Long]()
  var relLabelCount: mutable.Map[String, Long] = mutable.Map[String, Long]()
  var allNodesCount: Long = -1
  var allRelCount: Long = -1
  var propertyIndexCount: mutable.Map[String, Long] = mutable.Map[String, Long]()

  //labelCount += "student" -> 1
  //relLabelCount += "knows" -> 1
 // propertyIndexCount += ("student" -> "name") -> 1

  def start(): Unit = {

    //KeyHandler

    ByteUtils.stringFromBytes(rocksDB.get(ByteUtils.longToBytes(0))).split(",").foreach(nodeLabelSchema += _)
    ByteUtils.stringFromBytes(rocksDB.get(ByteUtils.longToBytes(1))).split(",").foreach(relLabelSchema += _)
    ByteUtils.stringFromBytes(rocksDB.get(ByteUtils.longToBytes(2))).split(",").foreach(indexPropertySchema += _)
    //rocksDB.get(ByteUtils.longToBytes(0)).toString.split(",").foreach(nodeLabelSchema += _)
    //rocksDB.get(ByteUtils.longToBytes(1)).toString.split(",").foreach(relLabelSchema += _)
    //rocksDB.get(ByteUtils.longToBytes(2)).toString.split(",").foreach(indexPropertySchema += _)

    //nodeLabelSchema.foreach(u => labelCount += u->rocksDB.get(u.getBytes).toString.toLong)
    nodeLabelSchema.foreach(u => labelCount += u->ByteUtils.getLong(rocksDB.get(u.getBytes), 0))
    relLabelSchema.foreach(u => relLabelCount += u->ByteUtils.getLong(rocksDB.get(u.getBytes), 0))
    indexPropertySchema.foreach(u => propertyIndexCount += u->ByteUtils.getLong(rocksDB.get(u.getBytes), 0))

    allNodesCount = ByteUtils.getLong(rocksDB.get(ByteUtils.longToBytes(3)), 0)
    allRelCount = ByteUtils.getLong(rocksDB.get(ByteUtils.longToBytes(4)), 0)



  }


  def save(): Unit = {
    rocksDB.put(ByteUtils.longToBytes(0), ByteUtils.stringToBytes(nodeLabelSchema.mkString(",")))
    rocksDB.put(ByteUtils.longToBytes(1), ByteUtils.stringToBytes(relLabelSchema.mkString(",")))
    rocksDB.put(ByteUtils.longToBytes(2), ByteUtils.stringToBytes(indexPropertySchema.mkString(",")))
    rocksDB.put(ByteUtils.longToBytes(3), ByteUtils.longToBytes(allNodesCount))
    rocksDB.put(ByteUtils.longToBytes(4), ByteUtils.longToBytes(allRelCount))

   /* rocksDB.put(ByteUtils.longToBytes(1), relLabelSchema.mkString(",").getBytes)
    rocksDB.put(ByteUtils.longToBytes(2), indexPropertySchema.mkString(",").getBytes)
    rocksDB.put(ByteUtils.longToBytes(3), allNodesCount.toString.getBytes)
    rocksDB.put(ByteUtils.longToBytes(4), allRelCount.toString.getBytes)*/


    labelCount.foreach(u => rocksDB.put(ByteUtils.stringToBytes(u._1), ByteUtils.longToBytes(u._2)))
    relLabelCount.foreach(u => rocksDB.put(ByteUtils.stringToBytes(u._1), ByteUtils.longToBytes(u._2)))
    propertyIndexCount.foreach(u => rocksDB.put(ByteUtils.stringToBytes(u._1), ByteUtils.longToBytes(u._2)))

  }


  def getAllNodesCount(): Long = {
    allNodesCount
  }

  def addNodes(count: Long): Unit ={
    allNodesCount += count
  }

  def addRels(count: Long): Unit ={
    allRelCount += count
  }

  def getAllRelCount(): Long = {
    allRelCount
  }

  def delNodes(count: Long): Unit ={
    allNodesCount -= count
  }


  def delRels(count: Long): Unit ={
    allRelCount -= count
  }

  def getLabelCount(label: String): Option[Long] = {
    labelCount.get(label)
  }

  def getRelLabelCount(label: String): Option[Long] = {
    relLabelCount.get(label)
  }

  def getPropertyIndexCount(label: String, property: String): Option[Long] = {
    propertyIndexCount.get(label+property)
  }

  def addNodeLabelCount(label: String, count: Long): Unit = {
    if (nodeLabelSchema.contains(label)){
      labelCount += label -> (labelCount.get(label).get + count)
    }
    else {
      nodeLabelSchema += label
      labelCount += label -> count
    }
    addNodes(count)
  }

  def delNodeLabelCount(label: String, count: Long): Unit = {
    if (nodeLabelSchema.contains(label)){
      labelCount += label -> (labelCount.get(label).get - count)
    }
    delNodes(count)
  }

  def addRelLabelCount(label: String, count: Long): Unit = {
    if (relLabelSchema.contains(label)){
      relLabelCount += label -> (relLabelCount.get(label).get + count)
    }
    else {
      relLabelSchema += label
      relLabelCount += label -> count
    }

    addRels(count)

  }

  def delRelLabelCount(label: String, count: Long): Unit = {
    if (relLabelSchema.contains(label)){
      relLabelCount += label -> (relLabelCount.get(label).get - count)
    }
    delRels(count)
  }

  def addPropertyIndexCount(label: String, property: String, count : Long): Unit = {
    if(indexPropertySchema.contains(label+property)) {
      propertyIndexCount += (label+property)->(propertyIndexCount.get(label+property).get + count)
    }
    else {
      indexPropertySchema += label+property
      propertyIndexCount += (label+property)->count
    }
  }

  def delPropertyIndexCount(label: String, property: String, count : Long): Unit = {
    if (indexPropertySchema.contains(label+property)){
      propertyIndexCount += (label+property)->(propertyIndexCount.get(label+property).get - count)
    }
  }

}
