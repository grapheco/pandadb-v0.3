/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 19:47 2020/12/3
 * @Modified By:
 */
object PDBMetaData {
  private var _propIdMap: Map[String, Int] = Map[String, Int]()
  private var _labelIdMap: Map[String, Int] = Map[String, Int]()
  private var _typeIdMap: Map[String, Int] = Map[String, Int]()

  private var _propCounter: Int = 0
  private var _labelCounter: Int = 0
  private var _typeCounter: Int = 0

  def isPropExists(prop: String): Boolean = _propIdMap.contains(prop)
  def isLabelExists(label: String): Boolean = _labelIdMap.contains(label)
  def isTypeExists(edgeType: String): Boolean = _typeIdMap.contains(edgeType)

  def addProp(prop: String): Int = {
    if(!isPropExists(prop)){
      _propIdMap += (prop -> _propCounter)
      _propCounter += 1
      _propCounter - 1
    } else _propIdMap(prop)
  }

  def addLabel(label: String): Int = {
    if(!isLabelExists(label)){
      _labelIdMap += (label -> _labelCounter)
      _labelCounter += 1
      _labelCounter - 1
    } else _labelIdMap(label)
  }

  def addType(edgeType: String): Int = {
    if(!isTypeExists(edgeType)){
      _typeIdMap += (edgeType -> _typeCounter)
      _typeCounter += 1
      _typeCounter - 1
    } else _typeIdMap(edgeType)
  }

  def getPropId(prop: String): Int = {
    if (isPropExists(prop)) _propIdMap.get(prop).get
    else addProp(prop)
  }

  def getLabelId(label: String): Int = {
    if (isLabelExists(label)) _labelIdMap.get(label).get
    else addLabel(label)
  }

  def getTypeId(edgeType: String): Int = {
    if (isTypeExists(edgeType)) _typeIdMap.get(edgeType).get
    else addType(edgeType)
  }
}
