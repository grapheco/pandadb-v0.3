package cn.pandadb.kernel.kv.lynx.procedure.functions

import java.text.SimpleDateFormat

import cn.pandadb.kernel.kv.lynx.procedure.PandaFunction
import org.grapheco.lynx.{CallableProcedure, LynxDate, LynxDateTime, LynxType, LynxValue}
import org.opencypher.v9_0.util.symbols.CTDate

case object DateTime extends PandaFunction{
  override def name: String = ".datetime"

  override def callableProcedure: Some[CallableProcedure] = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("datetime"->CTDate)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          Iterable(Seq(LynxDateTime(new SimpleDateFormat("yyyy-MM-dd").parse(args.head.value.toString).getTime)))
        }
      }
    )
  }
}
