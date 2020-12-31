package cn.pandadb.kernel.optimizer

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 19:20 2020/11/27
 * @Modified By:
 */

//carried from panda-2019
trait NFExpression {
}

trait NFPredicate extends NFExpression {
}

trait NFBinaryPredicate extends NFPredicate{
  //def propName: String
  def getName(): String

  def getPropName(): String

  def getValue(): AnyValue

  def getType(): String
  //def value: AnyValue
}

trait NFRangePredicate extends NFBinaryPredicate {

}

trait NFRangePredicateWithEqual extends NFRangePredicate{

}

case class NFBetween(propName: String, left: AnyValue, right: AnyValue, name: String, isLeftEqual:Boolean, isRightEqual: Boolean) extends NFPredicate {

}
case class NFLabels(labels: Seq[String] ) extends NFPredicate {

}

case class NFLimit(size: Long) extends NFPredicate {

}
case class NFGreaterThan(propName: String, value: AnyValue, name: String) extends NFRangePredicate{
  override def getName(): String = name

  override def getPropName(): String = propName

  override def getValue(): AnyValue = value

  override def getType(): String = ">"
}

case class NFGreaterThanOrEqual(propName: String, value: AnyValue, name: String) extends NFRangePredicateWithEqual {
  override def getName(): String = name

  override def getPropName(): String = propName

  override def getValue(): AnyValue = value

  override def getType(): String = ">="
}

case class NFLessThan(propName: String, value: AnyValue, name: String) extends NFRangePredicate {
  override def getName(): String = name

  override def getPropName(): String = propName

  override def getValue(): AnyValue = value

  override def getType(): String = "<"
}

case class NFLessThanOrEqual(propName: String, value: AnyValue, name: String) extends NFRangePredicateWithEqual {
  override def getName(): String = name

  override def getPropName(): String = propName

  override def getValue(): AnyValue = value

  override def getType(): String = "<="
}

case class NFEquals(propName: String, value: AnyValue, name: String) extends NFBinaryPredicate {
  override def getName(): String = name

  override def getPropName(): String = propName

  override def getValue(): AnyValue = value

  override def getType(): String = "="
}

case class NFNotEquals(propName: String, value: AnyValue, name: String) extends NFBinaryPredicate {
  override def getName(): String = name

  override def getPropName(): String = propName

  override def getValue(): AnyValue = value

  override def getType(): String = "!="
}

case class NFNotNull(propName: String) extends NFPredicate {
}

case class NFIsNull(propName: String) extends NFPredicate {
}

case class NFTrue() extends NFPredicate {
}

case class NFFalse() extends NFPredicate {
}

case class NFStartsWith(propName: String, text: String) extends NFPredicate {
}

case class NFEndsWith(propName: String, text: String) extends NFPredicate {
}

case class NFHasProperty(propName: String) extends NFPredicate {
}

case class NFContainsWith(propName: String, text: String) extends NFPredicate {
}

case class NFRegexp(propName: String, text: String) extends NFPredicate {
}

case class NFAnd(a: NFPredicate, b: NFPredicate) extends NFPredicate {
}

case class NFOr(a: NFPredicate, b: NFPredicate) extends NFPredicate {
}

case class NFNot(a: NFPredicate) extends NFPredicate {
}

case class NFConstantCachedIn(a: NFPredicate) extends NFPredicate {
}

case class AnyValue(anyValue: Any)
