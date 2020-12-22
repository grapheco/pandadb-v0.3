//package cn.pandadb.kv
//import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler}
//import cn.pandadb.kernel.kv.KeyHandler.KeyType
//import org.junit.{After, Assert, Before, Test}
//
///**
// * @Author: Airzihao
// * @Description:
// * @Date: Created at 11:59 2020/11/28
// * @Modified By:
// */
//class KeyHandlerTest {
//  @Test
//  def testNodeKey(): Unit = {
//    val nodeId: Long = 123
//    val bytes = KeyHandler.nodeKeyToBytes(nodeId)
//    Assert.assertEquals(9, bytes.length)
//    Assert.assertEquals(nodeId, KeyHandler.parseNodeKeyFromBytes(bytes))
//  }
//
//  @Test
//  def testNodeKeyPrefix(): Unit ={
//    val bytes = KeyHandler.nodeKeyPrefix()
//    Assert.assertEquals(1, bytes.length)
//    Assert.assertEquals(KeyHandler.KeyType.Node.id.toByte, bytes(0))
//  }
//
//  @Test
//  def testNodeLabelIndexKey(): Unit = {
//    val label: Int = 888
//    val nodeId: Long = 456
//    val bytes = KeyHandler.nodeLabelIndexKeyToBytes(label, nodeId)
//    Assert.assertEquals(13, bytes.length)
//    Assert.assertEquals(label, KeyHandler.parseNodeLabelIndexKeyFromBytes(bytes)._1)
//    Assert.assertEquals(nodeId, KeyHandler.parseNodeLabelIndexKeyFromBytes(bytes)._2)
//  }
//
//  @Test
//  def testNodeLabelIndexKeyPrefixToBytes(): Unit = {
//    val label: Int = 999
//    val bytes = KeyHandler.nodeLabelIndexKeyPrefixToBytes(label)
//    Assert.assertEquals(5, bytes.length)
//    Assert.assertEquals(KeyType.NodeLabelIndex.id.toByte, bytes(0))
//    Assert.assertEquals(999, ByteUtils.getInt(bytes, 1))
//  }
//
//  @Test
//  def testNodePropertyIndexMetaKeyToBytes(): Unit = {
//    val label: Int = 111
//    val props: Array[Int] = Array[Int](1, 2, 3, 4)
//    val bytes = KeyHandler.nodePropertyIndexMetaKeyToBytes(label, props)
//    Assert.assertEquals(1 + 4 + 4 * 4, bytes.length)
//    Assert.assertEquals(KeyType.NodePropertyIndexMeta.id, bytes(0))
//    Assert.assertEquals(111, ByteUtils.getInt(bytes, 1))
//    Assert.assertEquals(1, ByteUtils.getInt(bytes, 5))
//    Assert.assertEquals(2, ByteUtils.getInt(bytes, 9))
//    Assert.assertEquals(3, ByteUtils.getInt(bytes, 13))
//    Assert.assertEquals(4, ByteUtils.getInt(bytes, 17))
//  }
//
//  @Test
//  def testNodePropertyFulltextIndexMetaKeyToBytes(): Unit = {
//    val label: Int = 111
//    val props: Array[Int] = Array[Int](1, 2, 3, 4)
//    val bytes = KeyHandler.nodePropertyFulltextIndexMetaKeyToBytes(label, props)
//    Assert.assertEquals(1 + 4 + 4 * 4, bytes.length)
//    Assert.assertEquals(KeyType.NodePropertyFulltextIndexMeta.id, bytes(0))
//    Assert.assertEquals(111, ByteUtils.getInt(bytes, 1))
//    Assert.assertEquals(1, ByteUtils.getInt(bytes, 5))
//    Assert.assertEquals(2, ByteUtils.getInt(bytes, 9))
//    Assert.assertEquals(3, ByteUtils.getInt(bytes, 13))
//    Assert.assertEquals(4, ByteUtils.getInt(bytes, 17))
//  }
//
//  @Test
//  def testNodePropertyIndexKeyToBytes(): Unit = {
////    val indexId: Int = 1
////    val value: Array[Byte] = Array[Byte](1.toByte, 2.toByte)
////    val length: Array[Byte] = Array[Byte](2.toByte)
////    val nodeId: Long = 10
////    val bytes = KeyHandler.nodePropertyIndexKeyToBytes(indexId, value, length, nodeId)
////    Assert.assertEquals(5 + 2 + 1 + 8, bytes.length)
////    Assert.assertEquals(KeyType.NodePropertyIndex.id, bytes(0))
////    Assert.assertEquals(1, ByteUtils.getInt(bytes, 1))
////    Assert.assertEquals(1, ByteUtils.getByte(bytes, 5))
////    Assert.assertEquals(2, ByteUtils.getByte(bytes, 6))
////    Assert.assertEquals(2, ByteUtils.getByte(bytes, 7))
////    Assert.assertEquals(10, ByteUtils.getLong(bytes, 8))
//  }
//
//  @Test
//  def testNodePropertyIndexPrefixToBytes(): Unit = {
////    val indexId: Int = 1
////    val value: Array[Byte] = Array[Byte](1.toByte, 2.toByte)
////    val length: Array[Byte] = Array[Byte](2.toByte)
////    val bytes = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, value, length)
////    Assert.assertEquals(5 + 2 + 1, bytes.length)
////    Assert.assertEquals(KeyType.NodePropertyIndex.id, bytes(0))
////    Assert.assertEquals(1, ByteUtils.getInt(bytes, 1))
////    Assert.assertEquals(1, ByteUtils.getByte(bytes, 5))
////    Assert.assertEquals(2, ByteUtils.getByte(bytes, 6))
////    Assert.assertEquals(2, ByteUtils.getByte(bytes, 7))
//  }
//
//  @Test
//  def testRelationKeyToBytes(): Unit = {
//    val relationId: Long = 10
//    val bytes = KeyHandler.relationKeyToBytes(relationId)
//    KeyHandler.parseRelationKeyFromBytes(bytes)
//    Assert.assertEquals(relationId, KeyHandler.parseRelationKeyFromBytes(bytes))
//  }
//
//  @Test
//  def testRelationKeyPrefix(): Unit = {
//    val bytes = KeyHandler.relationKeyPrefix()
//    Assert.assertEquals(KeyType.Relation.id.toByte, bytes(0))
//  }
//
//  @Test
//  def testRelationLabelIndexKeyToBytes(): Unit = {
//    val relationId: Long = 10
//    val labelId: Int = 1
//    val bytes = KeyHandler.relationLabelIndexKeyToBytes(labelId, relationId)
//    Assert.assertEquals(labelId, KeyHandler.parseRelationLabelIndexKeyFromBytes(bytes)._1)
//    Assert.assertEquals(relationId, KeyHandler.parseRelationLabelIndexKeyFromBytes(bytes)._2)
//  }
//
//  @Test
//  def testRelationLabelIndexKeyPrefixToBytes(): Unit = {
//    val labelId: Int = 1
//    val bytes = KeyHandler.relationLabelIndexKeyPrefixToBytes(labelId)
//    Assert.assertEquals(KeyType.RelationLabelIndex.id.toByte, bytes(0))
//    Assert.assertEquals(labelId, ByteUtils.getInt(bytes, 1))
//  }
//
//  @Test
//  def testOutEdgeKeyToBytes(): Unit = {
//    val fromNode: Long = 1
//    val labelId: Int = 2
//    val category: Long = 3
//    val toNodeId: Long = 4
//    val bytes = KeyHandler.outEdgeKeyToBytes(fromNode, labelId, category, toNodeId)
//    Assert.assertEquals(fromNode, KeyHandler.parseOutEdgeKeyFromBytes(bytes)._1)
//    Assert.assertEquals(labelId, KeyHandler.parseOutEdgeKeyFromBytes(bytes)._2)
//    Assert.assertEquals(category, KeyHandler.parseOutEdgeKeyFromBytes(bytes)._3)
//    Assert.assertEquals(toNodeId, KeyHandler.parseOutEdgeKeyFromBytes(bytes)._4)
//  }
//
//  @Test
//  def testOutEdgeKeyPrefixToBytes(): Unit = {
//
//    val bytes1 = KeyHandler.outEdgeKeyPrefixToBytes()
//    val bytes2 = KeyHandler.outEdgeKeyPrefixToBytes(1)
//    val bytes3 = KeyHandler.outEdgeKeyPrefixToBytes(1, 2)
//    val bytes4 = KeyHandler.outEdgeKeyPrefixToBytes(1, 2, 3)
//
//    Assert.assertEquals(KeyType.OutEdge.id.toByte, bytes1(0))
//    Assert.assertEquals(KeyType.OutEdge.id.toByte, bytes2(0))
//    Assert.assertEquals(KeyType.OutEdge.id.toByte, bytes3(0))
//    Assert.assertEquals(KeyType.OutEdge.id.toByte, bytes4(0))
//
//    Assert.assertEquals(1, ByteUtils.getLong(bytes2, 1))
//    Assert.assertEquals(1, ByteUtils.getLong(bytes3, 1))
//    Assert.assertEquals(1, ByteUtils.getLong(bytes4, 1))
//
//    Assert.assertEquals(2, ByteUtils.getInt(bytes3, 9))
//    Assert.assertEquals(2, ByteUtils.getInt(bytes4, 9))
//
//    Assert.assertEquals(3, ByteUtils.getLong(bytes4, 13))
//  }
//  ////
//
//  @Test
//  def testInEdgeKeyToBytes(): Unit = {
//    val fromNodeId: Long = 1
//    val labelId: Int = 2
//    val category: Long = 3
//    val toNodeId: Long = 4
//    val bytes = KeyHandler.inEdgeKeyToBytes(toNodeId, labelId, category, fromNodeId)
//    Assert.assertEquals(toNodeId, KeyHandler.parseInEdgeKeyFromBytes(bytes)._1)
//    Assert.assertEquals(labelId, KeyHandler.parseInEdgeKeyFromBytes(bytes)._2)
//    Assert.assertEquals(category, KeyHandler.parseInEdgeKeyFromBytes(bytes)._3)
//    Assert.assertEquals(fromNodeId, KeyHandler.parseInEdgeKeyFromBytes(bytes)._4)
//  }
//
//  @Test
//  def testInEdgeKeyPrefixToBytes(): Unit = {
//
//    val bytes1 = KeyHandler.inEdgeKeyPrefixToBytes()
//    val bytes2 = KeyHandler.inEdgeKeyPrefixToBytes(1)
//    val bytes3 = KeyHandler.inEdgeKeyPrefixToBytes(1, 2)
//    val bytes4 = KeyHandler.inEdgeKeyPrefixToBytes(1, 2, 3)
//
//    Assert.assertEquals(KeyType.InEdge.id.toByte, bytes1(0))
//    Assert.assertEquals(KeyType.InEdge.id.toByte, bytes2(0))
//    Assert.assertEquals(KeyType.InEdge.id.toByte, bytes3(0))
//    Assert.assertEquals(KeyType.InEdge.id.toByte, bytes4(0))
//
//    Assert.assertEquals(1, ByteUtils.getLong(bytes2, 1))
//    Assert.assertEquals(1, ByteUtils.getLong(bytes3, 1))
//    Assert.assertEquals(1, ByteUtils.getLong(bytes4, 1))
//
//    Assert.assertEquals(2, ByteUtils.getInt(bytes3, 9))
//    Assert.assertEquals(2, ByteUtils.getInt(bytes4, 9))
//
//    Assert.assertEquals(3, ByteUtils.getLong(bytes4, 13))
//  }
//}
