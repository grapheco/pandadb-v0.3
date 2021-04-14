package cn.pandadb.kernel.kv.value

import java.util.Date

import org.grapheco.lynx.{InvalidValueException, LynxBoolean, LynxDate, LynxDouble, LynxInteger, LynxList, LynxMap, LynxNull, LynxString, LynxValue}

object ValueMappings {

  def lynxValueMappingToScala(value:LynxValue): Any = {
    value match {
      case LynxNull => null
      case v: LynxBoolean => v.value
      case v: LynxInteger => v.value
      case v: LynxString => v.value
      case v: LynxDouble => v.value
      case v: LynxDate => new Date(v.value)
      case v: LynxList => v.value.map(lynxValueMappingToScala(_)).toArray
      case _ => throw InvalidValueException(value)
    }
  }

}
