package cn.pandadb.kernel.kv.lynx.procedure.functions

import java.text.SimpleDateFormat

import cn.pandadb.kernel.kv.lynx.procedure.PandaFunction
import org.grapheco.lynx.{CallableProcedure, LynxDate, LynxInteger, LynxType, LynxValue}
import org.opencypher.v9_0.util.symbols.CTInteger

case object Count extends PandaFunction{
  override def name: String = ".count"

  override def callableProcedure: Some[CallableProcedure] = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("count"->CTInteger)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          Iterable(Seq(LynxInteger(args.size)))
        }
      }
    )
  }
}
