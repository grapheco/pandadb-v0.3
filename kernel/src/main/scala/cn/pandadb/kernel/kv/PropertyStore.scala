package cn.pandadb.kernel.kv

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 19:28 2020/11/27
 * @Modified By:
 */
trait PropertyWriter {
  def addProperty(nodeId: Long, key: String, value: PropertyValue): Unit;

  def removeProperty(nodeId: Long, key: String);

  def updateProperty(nodeId: Long, key: String, value: PropertyValue): Unit;
}

class PropertyValue {

}
