//package cn.pandadb.driver
//
//import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime, ZonedDateTime}
//import java.{lang, util}
//import java.util.function
//
//import cn.pandadb.hipporpc.utils.DriverValue
//import cn.pandadb.hipporpc.values.{Value => HippoValue}
//
//import scala.collection.JavaConverters._
//import org.neo4j.blob.Blob
//import org.neo4j.driver.Value
//import org.neo4j.driver.types.{Entity, IsoDuration, Node, Path, Point, Relationship, Type}
//
//class PandaValue(driverValue: DriverValue) extends Value {
//  override def size(): Int = driverValue.value.size
//
//  override def isEmpty: Boolean = driverValue.value.isEmpty
//
//  override def keys(): lang.Iterable[String] = asJavaIterable(driverValue.keys)
//
//  override def get(index: Int): Value =
//
//  override def `type`(): Type = ???
//
//  override def hasType(`type`: Type): Boolean = `type`.name() == value.getType()
//
//  override def isTrue: Boolean = ???
//
//  override def isFalse: Boolean = ???
//
//  override def isNull: Boolean = ???
//
//  override def asObject(): AnyRef = ???
//
//  override def computeOrDefault[T](mapper: function.Function[Value, T], defaultValue: T): T = ???
//
//  override def asBoolean(): Boolean = ???
//
//  override def asBoolean(defaultValue: Boolean): Boolean = ???
//
//  override def asByteArray(): Array[Byte] = ???
//
//  override def asByteArray(defaultValue: Array[Byte]): Array[Byte] = ???
//
//  override def asString(): String = ???
//
//  override def asString(defaultValue: String): String = ???
//
//  override def asNumber(): Number = ???
//
//  override def asLong(): Long = ???
//
//  override def asBlob(): Blob = ???
//
//  override def asLong(defaultValue: Long): Long = ???
//
//  override def asInt(): Int = ???
//
//  override def asInt(defaultValue: Int): Int = ???
//
//  override def asDouble(): Double = ???
//
//  override def asDouble(defaultValue: Double): Double = ???
//
//  override def asFloat(): Float = ???
//
//  override def asFloat(defaultValue: Float): Float = ???
//
//  override def asList(): util.List[AnyRef] = ???
//
//  override def asList(defaultValue: util.List[AnyRef]): util.List[AnyRef] = ???
//
//  override def asList[T](mapFunction: function.Function[Value, T]): util.List[T] = ???
//
//  override def asList[T](mapFunction: function.Function[Value, T], defaultValue: util.List[T]): util.List[T] = ???
//
//  override def asEntity(): Entity = ???
//
//  override def asNode(): Node = ???
//
//  override def asRelationship(): Relationship = ???
//
//  override def asPath(): Path = ???
//
//  override def asLocalDate(): LocalDate = ???
//
//  override def asOffsetTime(): OffsetTime = ???
//
//  override def asLocalTime(): LocalTime = ???
//
//  override def asLocalDateTime(): LocalDateTime = ???
//
//  override def asOffsetDateTime(): OffsetDateTime = ???
//
//  override def asZonedDateTime(): ZonedDateTime = ???
//
//  override def asIsoDuration(): IsoDuration = ???
//
//  override def asPoint(): Point = ???
//
//  override def asLocalDate(defaultValue: LocalDate): LocalDate = ???
//
//  override def asOffsetTime(defaultValue: OffsetTime): OffsetTime = ???
//
//  override def asLocalTime(defaultValue: LocalTime): LocalTime = ???
//
//  override def asLocalDateTime(defaultValue: LocalDateTime): LocalDateTime = ???
//
//  override def asOffsetDateTime(defaultValue: OffsetDateTime): OffsetDateTime = ???
//
//  override def asZonedDateTime(defaultValue: ZonedDateTime): ZonedDateTime = ???
//
//  override def asIsoDuration(defaultValue: IsoDuration): IsoDuration = ???
//
//  override def asPoint(defaultValue: Point): Point = ???
//
//  override def asMap(defaultValue: util.Map[String, AnyRef]): util.Map[String, AnyRef] = ???
//
//  override def asMap[T](mapFunction: function.Function[Value, T], defaultValue: util.Map[String, T]): util.Map[String, T] = ???
//
//  override def containsKey(key: String): Boolean = ???
//
//  override def get(key: String): Value = ???
//
//  override def values(): lang.Iterable[Value] = ???
//
//  override def values[T](mapFunction: function.Function[Value, T]): lang.Iterable[T] = ???
//
//  override def asMap(): util.Map[String, AnyRef] = ???
//
//  override def asMap[T](mapFunction: function.Function[Value, T]): util.Map[String, T] = ???
//
//  override def get(key: String, defaultValue: Value): Value = ???
//
//  override def get(key: String, defaultValue: Any): AnyRef = ???
//
//  override def get(key: String, defaultValue: Number): Number = ???
//
//  override def get(key: String, defaultValue: Entity): Entity = ???
//
//  override def get(key: String, defaultValue: Node): Node = ???
//
//  override def get(key: String, defaultValue: Path): Path = ???
//
//  override def get(key: String, defaultValue: Relationship): Relationship = ???
//
//  override def get(key: String, defaultValue: util.List[AnyRef]): util.List[AnyRef] = ???
//
//  override def get[T](key: String, defaultValue: util.List[T], mapFunc: function.Function[Value, T]): util.List[T] = ???
//
//  override def get(key: String, defaultValue: util.Map[String, AnyRef]): util.Map[String, AnyRef] = ???
//
//  override def get[T](key: String, defaultValue: util.Map[String, T], mapFunc: function.Function[Value, T]): util.Map[String, T] = ???
//
//  override def get(key: String, defaultValue: Int): Int = ???
//
//  override def get(key: String, defaultValue: Long): Long = ???
//
//  override def get(key: String, defaultValue: Boolean): Boolean = ???
//
//  override def get(key: String, defaultValue: String): String = ???
//
//  override def get(key: String, defaultValue: Float): Float = ???
//
//  override def get(key: String, defaultValue: Double): Double = ???
//}
