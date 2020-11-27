package cn.pandadb.kernel.kv

class NodeStore {

}

trait NodeWriter {
  def deleteNode(nodeId: Long);

  def addNode(nodeId: Long);

  def addLabel(nodeId: Long, label: Label): Unit;

  def removeLabel(nodeId: Long, label: Label): Unit;
}
