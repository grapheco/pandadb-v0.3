package cn.pandadb.kernel.transaction

object DBNameMap {
  val nodeDB = "node"
  val nodeLabelDB = "nodeLabel"
  val nodeMetaDB = "nodeMeta"
  val relationDB = "relation"
  val inRelationDB = "inRelation"
  val outRelationDB = "outRelation"
  val relationLabelDB = "relationLabel"
  val relationMetaDB = "relationMeta"
  val statisticsDB = "statistics"
  val indexDB = "index"
  val indexMetaDB = "indexMeta"

  val undoLogName = "undoLog.txt"
  val guardLogName = "guardLog.txt"
}
