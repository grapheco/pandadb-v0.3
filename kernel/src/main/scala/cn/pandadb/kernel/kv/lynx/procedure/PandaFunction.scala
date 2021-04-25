package cn.pandadb.kernel.kv.lynx.procedure
import cn.pandadb.kernel.kv.lynx.procedure.functions.{DateFunction, DateTimeFunction, ExistsFunction, IdFunction, LocalDateTimeFunction, LocalTimeFunction, TimeFunction, ToIntegerFunction, TypeFunction}
import org.grapheco.lynx.CallableProcedure

object PandaFunction {
  private val knownFunctions: Seq[PandaFunction] = Vector(
    ToIntegerFunction,
    DateFunction,
    DateTimeFunction,
    LocalDateTimeFunction,
    TimeFunction,
    LocalTimeFunction,
    TypeFunction,
    IdFunction,
    ExistsFunction
      )
  val lookup: Map[String, PandaFunction] = knownFunctions.map(f => (f.name.toLowerCase, f)).toMap
}

abstract class PandaFunction{
  def name: String
  def callableProcedure: Some[CallableProcedure]
}