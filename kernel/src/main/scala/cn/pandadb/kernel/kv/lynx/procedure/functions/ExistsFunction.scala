package cn.pandadb.kernel.kv.lynx.procedure.functions

import cn.pandadb.kernel.kv.lynx.procedure.PandaFunction
import org.grapheco.lynx.{CallableProcedure, LynxBoolean, LynxInteger, LynxType, LynxValue}
import org.opencypher.v9_0.util.symbols.CTBoolean

case object ExistsFunction extends PandaFunction{
  override def name: String = ".exists"
  val procedure = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("exists"->CTBoolean)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          Iterable(Seq(LynxBoolean(args.head != None)))
        }
      }
    )
  }
  override def callableProcedure: Some[CallableProcedure] = {
   procedure
  }
}
