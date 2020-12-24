//package cn.pandadb.kernel.kv.meta
//
//import cn.pandadb.kernel.kv.ByteUtils
//import cn.pandadb.kernel.util.serializer.BaseSerializer
//import org.rocksdb.RocksDB
//
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer
//
//
//
//class Statistics(rocksDB: RocksDB) {
//
//  /*
//   0->label: student, person...
//   1->rellabel: fanOf, knows...
//   2->indexProperty: student+age, person+name
//   3->allNodesCount
//   4->allRelationCount
//     ....
//     ....
//  */
//  //private val labelStr = "label"
//  var nodeLabelSchema: mutable.Set[Int] = mutable.Set[Int]()
//  var relLabelSchema: mutable.Set[Int] = mutable.Set[Int]()
//  var indexPropertySchema: mutable.Set[Int] = mutable.Set[Int]()
//
//  var labelCount: mutable.Map[Int, Long] = mutable.Map[Int, Long]()
//  var relLabelCount: mutable.Map[Int, Long] = mutable.Map[Int, Long]()
//  var allNodesCount: Long = -1
//  var allRelationCount: Long = -1
////  var propertyIndexCount: mutable.Map[Int, Long] = mutable.Map[Int, Long]()
//
//  //labelCount += "student" -> 1
//  //relLabelCount += "knows" -> 1
// // propertyIndexCount += ("student" -> "name") -> 1
//
//  def start(): Unit = {
//
//    //KeyHandler
//    ByteUtils.stringFromBytes(rocksDB.get(ByteUtils.longToBytes(0))).split(",").foreach(nodeLabelSchema += _)
//    ByteUtils.stringFromBytes(rocksDB.get(ByteUtils.longToBytes(1))).split(",").foreach(relLabelSchema += _)
//    ByteUtils.stringFromBytes(rocksDB.get(ByteUtils.longToBytes(2))).split(",").foreach(indexPropertySchema += _)
//
//    //nodeLabelSchema.foreach(u => labelCount += u->rocksDB.get(u.getBytes).toString.toLong)
//    nodeLabelSchema.foreach(u => labelCount += u->ByteUtils.getLong(rocksDB.get(u.getBytes), 0))
//    relLabelSchema.foreach(u => relLabelCount += u->ByteUtils.getLong(rocksDB.get(u.getBytes), 0))
//    indexPropertySchema.foreach(u => propertyIndexCount += u->ByteUtils.getLong(rocksDB.get(u.getBytes), 0))
//
//    allNodesCount = ByteUtils.getLong(rocksDB.get(ByteUtils.longToBytes(3)), 0)
//    allRelationCount = ByteUtils.getLong(rocksDB.get(ByteUtils.longToBytes(4)), 0)
//
//
//
//  }
//
//
//  def save(): Unit = {
//    rocksDB.put(ByteUtils.longToBytes(0), ByteUtils.stringToBytes(nodeLabelSchema.mkString(",")))
//    rocksDB.put(ByteUtils.longToBytes(1), ByteUtils.stringToBytes(relLabelSchema.mkString(",")))
//    rocksDB.put(ByteUtils.longToBytes(2), .stringToBytes(indexPropertySchema.mkString(",")))
//    rocksDB.put(ByteUtils.longToBytes(3), ByteUtils.longToBytes(allNodesCount))
//    rocksDB.put(ByteUtils.longToBytes(4), ByteUtils.longToBytes(allRelationCount))
//
//
//    labelCount.foreach(u => rocksDB.put(ByteUtils.stringToBytes(u._1), ByteUtils.longToBytes(u._2)))
//    relLabelCount.foreach(u => rocksDB.put(ByteUtils.stringToBytes(u._1), ByteUtils.longToBytes(u._2)))
////    propertyIndexCount.foreach(u => rocksDB.put(ByteUtils.stringToBytes(u._1), ByteUtils.longToBytes(u._2)))
//
//  }
//
//
//  def nodeCount: Long = allNodesCount
//
//  def increaseNodeCount(count: Long): Unit = allNodesCount += count
//
//  def decreaseNodes(count: Long): Unit = allNodesCount -= count
//
//  def relationCount: Long = allRelationCount
//
//  def increaseRelationCount(count: Long): Unit = allRelationCount += count
//
//  def decreaseRelations(count: Long): Unit = allRelationCount -= count
//
//  def nodeLabelCount(label: Int): Option[Long] = labelCount.get(label)
//
//  def increaseNodeLabelCount(label: Int, count: Long): Unit = {
//    nodeLabelSchema += label
//    labelCount += label -> (labelCount.getOrElse(label, 0) + count)
////    increaseNodes(count)
//  }
//
//  def decreaseNodeLabelCount(label: Int, count: Long): Unit = {
//    if (nodeLabelSchema.contains(label)){
//      labelCount += label -> (labelCount.getOrElse(label, 0) - count)
//    }
////    decreaseNodes(count)
//  }
//
//  def relationTypeCount(label: Int): Option[Long] = relLabelCount.get(label)
//
//  def increaseRelationTypeCount(label: Int, count: Long): Unit = {
//    relLabelSchema += label
//    relLabelCount += label -> (relLabelCount.getOrElse(label, 0) + count)
////    increaseRelations(count)
//  }
//
//  def decreaseRelationLabelCount(label: Int, count: Long): Unit = {
//    if (relLabelSchema.contains(label)){
//      relLabelCount += label -> (relLabelCount.getOrElse(label, 0) - count)
//    }
//    decreaseRelations(count)
//  }
//
////  def propertyIndexCount(label: Int, property: Int): Option[Long] = propertyIndexCount.get(label+property)
////
////
////  def increasePropertyIndexCount(label: String, property: String, count : Long): Unit = {
////    if(indexPropertySchema.contains(label+property)) {
////      propertyIndexCount += (label+property)->(propertyIndexCount.get(label+property).get + count)
////    }
////    else {
////      indexPropertySchema += label+property
////      propertyIndexCount += (label+property)->count
////    }
////  }
////
////  def decreasePropertyIndexCount(label: String, property: String, count : Long): Unit = {
////    if (indexPropertySchema.contains(label+property)){
////      propertyIndexCount += (label+property)->(propertyIndexCount.get(label+property).get - count)
////    }
////  }
//
//}
