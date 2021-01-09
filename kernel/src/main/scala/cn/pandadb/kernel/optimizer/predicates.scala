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
  def isOkValue(v:Any): Boolean
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
  def isInRange(v: Any): Boolean
}

trait NFRangePredicateWithEqual extends NFRangePredicate{

}

case class NFBetween(propName: String, left: AnyValue, right: AnyValue, name: String, isLeftEqual:Boolean, isRightEqual: Boolean) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}
case class NFLabels(labels: Seq[String] ) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class NFLimit(size: Long) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}
case class NFGreaterThan(propName: String, value: AnyValue, name: String) extends NFRangePredicate{
  override def getName(): String = name

  override def getPropName(): String = propName

  override def getValue(): AnyValue = value

  override def getType(): String = ">"

  override def isInRange(v: Any): Boolean = {
    if (v.isInstanceOf[String]) {
      if (value.anyValue.isInstanceOf[String]) v.asInstanceOf[String] > value.anyValue.asInstanceOf[String]
      else false
    }
    else {
      val vs = v match {
        case x: Long => x
        case x: Int => x
        case x: Float => x
        case x: Double => x
      }
      val vp = value.anyValue match {
        case x: Long => x
        case x: Int => x
        case x: Float => x
        case x: Double => x
      }
      if (vs > vp) true
      else false
    }

  }

  override def isOkValue(v: Any): Boolean = ???
}

case class NFGreaterThanOrEqual(propName: String, value: AnyValue, name: String) extends NFRangePredicateWithEqual {
  override def getName(): String = name

  override def getPropName(): String = propName

  override def getValue(): AnyValue = value

  override def getType(): String = ">="

  override def isInRange(v: Any): Boolean = {
    if (v.isInstanceOf[String]) {
      if (value.anyValue.isInstanceOf[String]) v.asInstanceOf[String] >= value.anyValue.asInstanceOf[String]
      else false
    }
    else {
      val vs = v match {
        case x: Long => x
        case x: Int => x
        case x: Float => x
        case x: Double => x
      }
      val vp = value.anyValue match {
        case x: Long => x
        case x: Int => x
        case x: Float => x
        case x: Double => x
      }
      if (vs >= vp) true
      else false
    }

  }

  override def isOkValue(v: Any): Boolean = ???
}

case class NFLessThan(propName: String, value: AnyValue, name: String) extends NFRangePredicate {
  override def getName(): String = name

  override def getPropName(): String = propName

  override def getValue(): AnyValue = value

  override def getType(): String = "<"

  override def isInRange(v: Any): Boolean = {
    if (v.isInstanceOf[String]) {
      if (value.anyValue.isInstanceOf[String]) v.asInstanceOf[String] < value.anyValue.asInstanceOf[String]
      else false
    }
    else {
      val vs = v match {
        case x: Long => x
        case x: Int => x
        case x: Float => x
        case x: Double => x
      }
      val vp = value.anyValue match {
        case x: Long => x
        case x: Int => x
        case x: Float => x
        case x: Double => x
      }
      if (vs < vp) true
      else false
    }
  }

  override def isOkValue(v: Any): Boolean = ???
}

case class NFLessThanOrEqual(propName: String, value: AnyValue, name: String) extends NFRangePredicateWithEqual {
  override def getName(): String = name

  override def getPropName(): String = propName

  override def getValue(): AnyValue = value

  override def getType(): String = "<="

  override def isInRange(v: Any): Boolean = {
    if (v.isInstanceOf[String]) {
      if (value.anyValue.isInstanceOf[String]) v.asInstanceOf[String] <= value.anyValue.asInstanceOf[String]
      else false
    }
    else {
      val vs = v match {
        case x: Long => x
        case x: Int => x
        case x: Float => x
        case x: Double => x
      }
      val vp = value.anyValue match {
        case x: Long => x
        case x: Int => x
        case x: Float => x
        case x: Double => x
      }
      if (vs <= vp) true
      else false
    }

  }

  override def isOkValue(v: Any): Boolean = ???
}

case class NFEquals(propName: String, value: AnyValue, name: String) extends NFBinaryPredicate {
  override def getName(): String = name

  override def getPropName(): String = propName

  override def getValue(): AnyValue = value

  override def getType(): String = "="

  override def isOkValue(v: Any): Boolean = ???
}

case class NFNotEquals(propName: String, value: AnyValue, name: String) extends NFBinaryPredicate {
  override def getName(): String = name

  override def getPropName(): String = propName

  override def getValue(): AnyValue = value

  override def getType(): String = "!="

  override def isOkValue(v: Any): Boolean = ???
}

case class NFNotNull(propName: String) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class NFIsNull(propName: String) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class NFTrue() extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class NFFalse() extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class NFStartsWith(propName: String, text: String) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class NFEndsWith(propName: String, text: String) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class NFHasProperty(propName: String) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class NFContainsWith(propName: String, text: String) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class NFRegexp(propName: String, text: String) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class NFAnd(a: NFPredicate, b: NFPredicate) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class NFOr(a: NFPredicate, b: NFPredicate) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class NFNot(a: NFPredicate) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class NFConstantCachedIn(a: NFPredicate) extends NFPredicate {
  override def isOkValue(v: Any): Boolean = ???
}

case class AnyValue(anyValue: Any)
