package org.grapheco.pandadb.net.rpc.values

import java.time._

import scala.collection.mutable.{ArrayBuffer, Buffer}

trait Value extends Serializable {

  def getType(): String = {
    throw new NotImplementException()
  }

  def isNull(): Boolean = {
    throw new NotImplementException()
  }

  def asAny(): Any = {
    throw new NotImplementException()
  }

  def asInt(): Int = {
    throw new NotImplementException()
  }

  def asLong(): Long = {
    throw new NotImplementException()
  }

  def asBoolean(): Boolean = {
    throw new NotImplementException()
  }

  def asFloat(): Double = {
    throw new NotImplementException()
  }

  def asString(): String = {
    throw new NotImplementException()
  }

  def asNode(): Node = {
    throw new NotImplementException()
  }

  def asBytes(): Byte = {
    throw new NotImplementException()
  }

  def asNumber(): Long = {
    throw new NotImplementException()
  }

  def asDate(): LocalDate = {
    throw new NotImplementException()
  }

  def asTime(): OffsetTime = {
    throw new NotImplementException()
  }

  def asDateTime(): ZonedDateTime = {
    throw new NotImplementException()
  }

  def asLocalTime(): LocalTime = {
    throw new NotImplementException()
  }

  def asLocalDateTime(): LocalDateTime = {
    throw new NotImplementException()
  }

  def asDuration(): Duration = {
    throw new NotImplementException()
  }

  def asPoint2D(): Point2D = {
    throw new NotImplementException()
  }

  def asPoint3D(): Point3D = {
    throw new NotImplementException()
  }

  def asList(): Buffer[Value] = {
    throw new NotImplementException()
  }

  def asMap(): Map[String, Value] = {
    throw new NotImplementException()
  }

  def asRelationship(): Relationship = {
    throw new NotImplementException()
  }

//  def asPath(): Path = {
//    throw new NotImplementException()
//  }

//  def asBlobEntry(): BlobEntry = {
//    throw new NotImplementException()
//  }

  override def toString: String = "Driver Base Value"
}

case class NotImplementException(e: String = "not implement error") extends Exception(e) {

}

case class AnyValue(value: Any) extends Value {
  override def getType(): String = Types.ANY.toString

  override def asAny(): Any = value

  override def toString(): String = "Driver AnyValue"
}

case class StringValue(value: String) extends Value {
  override def getType(): String = Types.STRING.toString

  override def asAny(): Any = value

  override def asString(): String = value

  override def toString: String = value
}

case class IntegerValue(value: Long) extends Value {
  override def getType(): String = Types.INTEGER.toString

  override def asAny(): Any = value

  override def asInt(): Int = value.asInstanceOf[Int]

  override def asLong(): Long = value

  override def toString: String = value.toString

}

case class FloatValue(value: Double) extends Value {
  override def getType(): String = Types.FLOAT.toString

  override def asAny(): Any = value

  override def asFloat(): Double = value

  override def toString: String = value.toString
}

case class BooleanValue(value: Boolean) extends Value {
  override def getType(): String = Types.BOOLEAN.toString

  override def asAny(): Any = value

  override def asBoolean(): Boolean = value.asInstanceOf[Boolean]

  override def toString: String = value.toString
}

object NullValue extends Value {
  val value = null

  override def getType(): String = Types.NULL.toString

  override def asAny(): Any = value

  override def isNull: Boolean = true

  override def toString(): String = {
    "Driver NullValue"
  }
}

case class NodeValue(value: Node) extends Value {
  override def getType(): String = Types.NODE.toString

  override def asAny(): Any = value

  override def asNode(): Node = value

  override def toString(): String = value.toString
}

case class BytesValue(value: Byte) extends Value {
  override def getType(): String = Types.BYTES.toString

  override def asAny(): Any = value

  override def asBytes(): Byte = value

  override def toString: String = value.toString
}

case class NumberValue(value: Number) extends Value {
  override def getType(): String = Types.NUMBER.toString

  override def asAny(): Any = value

  override def asLong(): Long = value.longValue()

  override def asFloat(): Double = value.doubleValue()

  override def asInt(): Int = value.intValue()

  override def asBytes(): Byte = value.byteValue()

  def asShort(): Short = value.shortValue()

  override def toString: String = value.toString
}

case class DateValue(value: LocalDate) extends Value {
  override def getType(): String = Types.DATE.toString

  override def asAny(): Any = value

  override def asDate(): LocalDate = value

  override def toString: String = value.toString
}

case class TimeValue(value: OffsetTime) extends Value {
  override def getType(): String = Types.TIME.toString

  override def asAny(): Any = value

  override def asTime(): OffsetTime = value

  override def toString: String = value.toString
}

case class DateTimeValue(value: ZonedDateTime) extends Value {
  override def getType(): String = Types.DATE_TIME.toString

  override def asAny(): Any = value

  override def asDateTime(): ZonedDateTime = value

  override def toString: String = value.toString
}

case class LocalTimeValue(value: LocalTime) extends Value {
  override def getType(): String = Types.LOCAL_TIME.toString

  override def asAny(): Any = value

  override def asLocalTime(): LocalTime = value

  override def toString: String = value.toString
}

case class LocalDateTimeValue(value: LocalDateTime) extends Value {
  override def getType(): String = Types.LOCAL_DATE_TIME.toString

  override def asAny(): Any = value

  override def asLocalDateTime(): LocalDateTime = value

  override def toString: String = value.toString
}

case class DurationValue(value: Duration) extends Value {
  override def getType(): String = Types.DURATION.toString

  override def asAny(): Any = value

  override def asDuration(): Duration = value

  override def toString: String = value.toString
}

case class PointValue(value: Point) extends Value {
  override def getType(): String = Types.POINT.toString

  override def asAny(): Any = value

  override def asPoint2D(): Point2D = new Point2D(value.srid, value.x, value.y)

  override def asPoint3D(): Point3D = new Point3D(value.srid, value.x, value.y, value.z)

  override def toString: String = value.toString
}

case class ListValue(value: ArrayBuffer[Value]) extends Value {
  override def getType(): String = Types.LIST.toString

  override def asAny(): Any = value

  override def asList(): Buffer[Value] = value

  override def toString: String = value.toString
}

case class MapValue(value: Map[String, Value]) extends Value {
  override def getType(): String = Types.MAP.toString

  override def asAny(): Any = value

  override def asMap(): Map[String, Value] = value

  override def toString: String = value.toString
}

case class RelationshipValue(value: Relationship) extends Value {
  override def getType(): String = Types.RELATIONSHIP.toString

  override def asAny(): Any = value

  override def asRelationship(): Relationship = value

  override def toString: String = value.toString
}

//case class PathValue(value: Path) extends Value {
//  override def getType(): String = Types.PATH.toString
//
//  override def asAny(): Any = value
//
//  override def asPath(): Path = value
//
//  override def toString: String = value.toString(value)
//}

//case class BlobEntryValue(value: BlobEntry) extends Value {
//  override def getType(): String = Types.BLOB_ENTRY.toString
//
//  override def asAny(): Any = value
//
//  override def asBlobEntry(): BlobEntry = value
//
//  override def toString: String = value.toString()
//}
