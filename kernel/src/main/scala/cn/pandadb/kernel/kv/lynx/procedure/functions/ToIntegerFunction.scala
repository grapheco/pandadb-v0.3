package cn.pandadb.kernel.kv.lynx.procedure.functions

import cn.pandadb.kernel.kv.lynx.procedure.PandaFunction
import org.grapheco.lynx.{CallableProcedure, LynxInteger, LynxType, LynxValue}
import org.opencypher.v9_0.util.symbols.CTInteger

case object ToIntegerFunction extends PandaFunction{
  override def name: String = ".toInteger"
  val procedure = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("toInteger"->CTInteger)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          Iterable(Seq(LynxInteger(args.head.value.toString.toInt)))
        }
      }
    )
  }
  override def callableProcedure: Some[CallableProcedure] = {
   procedure
  }
}
