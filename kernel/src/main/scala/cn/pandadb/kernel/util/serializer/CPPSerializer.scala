package cn.pandadb.kernel.util.serializer

//import cn.pandadb.kernel.store.StoredNodeWithProperty

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 9:04 下午 2021/6/4
 * @Modified By:
 */
class CPPSerializer {

  //nodes Functions
  @native def serialize(labelId: Int, nodeId: Long): Array[Byte]
//  @native def serialize(nodeValue: StoredNodeWithProperty): Array[Byte]
//  @native def serialize(nodeID: Long, labels: Array[Int], props: Map[Int, Any]): Array[Byte]
//  @native def deserializeNodeValue(byteArr: Array[Byte]): StoredNodeWithProperty
//  @native def deserialzieNodeKey(byteArr: Array[Byte]): Long

}