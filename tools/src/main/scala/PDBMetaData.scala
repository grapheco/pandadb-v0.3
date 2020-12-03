/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 19:47 2020/12/3
 * @Modified By:
 */
object PDBMetaData {
  private var _propIdMap: Map[String, Int] = Map[String, Int]()
  private var _labelIdMap: Map[String, Int] = Map[String, Int]()

  private var _propCounter: Int = 0
  private var _labelCounter: Int = 0

  def isPropExists(prop: String): Boolean = _propIdMap.contains(prop)
  def isLabelExists(label: String): Boolean = _propIdMap.contains(label)

  def addProp(prop: String): Int = {
    _propIdMap += (prop -> _propCounter)
    _propCounter += 1
    _propCounter - 1
  }

  def addLabel(label: String): Int = {
    _labelIdMap += (label -> _labelCounter)
    _labelCounter += 1
    _labelCounter - 1
  }

  def getPropId(prop: String): Int = {
    if (isPropExists(prop)) _propIdMap.get(prop).get
    else addProp(prop)
  }

  def getLabelId(label: String): Int = {
    if (isLabelExists(label)) _labelIdMap.get(label).get
    else addLabel(label)
  }
}
