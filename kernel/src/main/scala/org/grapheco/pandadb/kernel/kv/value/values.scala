package org.grapheco.pandadb.kernel.kv.value

import org.grapheco.lynx.{InvalidValueException, LynxBoolean, LynxDate, LynxDateTime, LynxDouble, LynxInteger, LynxList, LynxLocalDateTime, LynxLocalTime, LynxMap, LynxNull, LynxString, LynxTime, LynxValue}

object ValueMappings {

  def lynxValueMappingToScala(value:LynxValue): Any = {
    value match {
      case LynxNull => null
      case v: LynxBoolean => v.value
      case v: LynxInteger => v.value
      case v: LynxString => v.value
      case v: LynxDouble => v.value
      case v: LynxDate => v.value
      case v: LynxDateTime => v.value
      case v: LynxLocalDateTime => v.value
      case v: LynxTime => v.value
      case v: LynxLocalTime => v.value
//      case v: LynxBlob => v.value
      case v: LynxList => v.value.map(lynxValueMappingToScala(_)).toArray
      case _ => throw InvalidValueException(value)
    }
  }
}

