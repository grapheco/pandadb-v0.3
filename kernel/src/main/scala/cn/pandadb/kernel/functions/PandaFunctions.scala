package cn.pandadb.kernel.functions

import cn.pandadb.kernel.store.{PandaNode, PandaRelationship}
import cn.pandadb.kernel.util.PandaDBException.PandaDBException
import org.grapheco.lynx.{CallableProcedure, DefaultProcedureRegistry, DefaultProcedures, DefaultTypeSystem, LynxBoolean, LynxList, LynxMap, LynxNull, LynxType, LynxValue}
import org.opencypher.v9_0.util.symbols._

object PandaFunctions {
  val pandaFunctions = new DefaultProcedureRegistry(new DefaultTypeSystem(), classOf[DefaultProcedures])

  def register(): DefaultProcedureRegistry ={
    pandaFunctions.register("keys", 1, new CallableProcedure {
      override val inputs: Seq[(String, LynxType)] = Seq("text" -> CTString)
      override val outputs: Seq[(String, LynxType)] = Seq("key_list" -> CTList(CTString))

      override def call(args: Seq[LynxValue]): LynxValue = {
        LynxValue(
          args.map {
          case n: PandaNode => n.properties.keys.toSeq
          case r: PandaRelationship => r.properties.keys.toSeq
          case m: LynxMap => m.value.keys.toSeq
          case _ => throw new PandaDBException("keys can only used on node, relationship and map")
        }.head
        )
      }
    })

    pandaFunctions.register("exists", 1, new CallableProcedure {
      override val inputs: Seq[(String, LynxType)] = Seq("text" -> CTString)
      override val outputs: Seq[(String, LynxType)] = Seq("exists" -> CTBoolean)

      override def call(args: Seq[LynxValue]): LynxValue = {
        LynxBoolean(args.head != LynxNull)
      }
    })

    pandaFunctions
  }
}
