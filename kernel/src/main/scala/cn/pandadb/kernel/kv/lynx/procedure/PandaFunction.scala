package cn.pandadb.kernel.kv.lynx.procedure
import cn.pandadb.kernel.kv.lynx.procedure.functions.{Date, DateTime, ToInteger, Type, Count}
import org.grapheco.lynx.CallableProcedure

object PandaFunction {
  private val knownFunctions: Seq[PandaFunction] = Vector(
    ToInteger,
    Date,
    DateTime,
    Type,
    Count
  )
  val lookup: Map[String, PandaFunction] = knownFunctions.map(f => (f.name.toLowerCase, f)).toMap
}

abstract class PandaFunction{
  def name: String
  def callableProcedure: Some[CallableProcedure]
}