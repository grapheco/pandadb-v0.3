package org.grapheco.pandadb.kernel.distribute.index.encoding.encoders

import org.grapheco.pandadb.kernel.distribute.DistributedKeyConverter
import org.grapheco.pandadb.kernel.distribute.index.PandaDistributedIndexStore
import org.grapheco.pandadb.kernel.distribute.index.encoding.{IndexEncoder, IndexEncoderNames}
import org.grapheco.pandadb.kernel.distribute.meta.NameMapping
import org.grapheco.pandadb.kernel.kv.ByteUtils

/**
 * @program: pandadb-v0.3
 * @description: tree encoding should start from root then layer by layer encode. can not cross layers.
 * @author: LiamGao
 * @create: 2022-01-06 13:44
 */
class TreeEncoder(indexStore: PandaDistributedIndexStore) extends IndexEncoder{
  type NodeId = Long
  type Code = String

  val db = indexStore.getDB()

  private val processor = indexStore.getIndexTool().getBulkProcessor(1000, 2)


  private val indexName = NameMapping.indexName
  private var treeFiledName: String = _
  private var labelName: String = _
  private var rootNodeId: String = _

  private var currentParent: (NodeId, Code) = _
  private var lastNodeInfo: (NodeId, Code) = _
  private var currentEncodeNum: Int = 1

  private var labelHasIndex: Boolean = _



  def init(_labelName: String, _rootNodeId: Long): Unit ={
    labelName = _labelName
    treeFiledName = s"$labelName.tree_encode"
    rootNodeId = _rootNodeId.toString
    currentParent = (_rootNodeId, getTreeCode())
    labelHasIndex = indexStore.getIndexedMetaData().contains(_labelName)

    indexStore.getIndexTool().setIndexToBatchMode(indexName)

    // add treeCode to root node
    val initRequest = {
      if (labelHasIndex) indexStore.addExtraProperty(indexName, _rootNodeId, Map((treeFiledName, currentParent._2)))
      else {
        indexStore.addNewNodeRequest(indexName, _rootNodeId, Seq(labelName), Map((treeFiledName, currentParent._2)))
      }
    }
    processor.add(initRequest)

  }

  def close(): Unit ={
    processor.flush()
    processor.close()
    indexStore.getIndexTool().setIndexToNormalMode(indexName)
  }

  /**
   * Assign the encoding start ID from db.
   */
  def getTreeCode(): String = {
    val meta = indexStore.getEncodingMetaData()
    if (meta.contains(treeFiledName)){
      val code = ByteUtils.getInt(meta(treeFiledName), 0)
      indexStore.setEncodingMeta(treeFiledName, ByteUtils.intToBytes(code + 1))
      code.toString
    }
    else {
      indexStore.setEncodingMeta(treeFiledName, ByteUtils.intToBytes(1))
      "1"
    }
  }

  /**
   * start tree encode.
   *
   * @param parentNodeId
   * @param childNodeId
   */
  def encode(parentNodeId: Long, childNodeId: Long): Unit ={
    if (currentParent._1 != parentNodeId){
      currentParent = lastNodeInfo
      currentEncodeNum = 1
    }
    val request = {
      val code = s"${currentParent._2}-$currentEncodeNum"
      currentEncodeNum += 1
      lastNodeInfo = (childNodeId, code)
      if (labelHasIndex) indexStore.addExtraProperty(indexName, childNodeId, Map((treeFiledName, code)))
      else indexStore.addNewNodeRequest(indexName, childNodeId, Seq(labelName), Map((treeFiledName, code)))
    }
    processor.add(request)
  }


}
