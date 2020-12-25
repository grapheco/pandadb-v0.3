package cn.pandadb.kernel.kv

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

}

case class NFLabels(labels: Seq[String] ) extends NFPredicate {

}

case class NFLimit(size: Long) extends NFPredicate {

}
case class NFGreaterThan(propName: String, value: AnyValue) extends NFBinaryPredicate{
}

case class NFGreaterThanOrEqual(propName: String, value: AnyValue) extends NFBinaryPredicate {
}

case class NFLessThan(propName: String, value: AnyValue) extends NFBinaryPredicate {
}

case class NFLessThanOrEqual(propName: String, value: AnyValue) extends NFBinaryPredicate {
}

case class NFEquals(propName: String, value: AnyValue) extends NFBinaryPredicate {
}

case class NFNotEquals(propName: String, value: AnyValue) extends NFBinaryPredicate {
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