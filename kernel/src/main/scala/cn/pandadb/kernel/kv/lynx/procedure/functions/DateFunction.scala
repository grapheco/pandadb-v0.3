package cn.pandadb.kernel.kv.lynx.procedure.functions

import java.text.SimpleDateFormat

import cn.pandadb.kernel.kv.lynx.procedure.PandaFunction
import org.grapheco.lynx.{CallableProcedure, LynxDate, LynxInteger, LynxString, LynxType, LynxValue}
import org.opencypher.v9_0.util.symbols.CTDate

case object DateFunction extends PandaFunction{
  override def name: String = ".date"
  val procedure = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("date"->CTDate)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          //TODO: support like yyyy/MM/dd HH:mm:ss
          Iterable(Seq(LynxDate(new SimpleDateFormat("yyyy-MM-dd").parse(args.head.value.toString).getTime)))
        }
      }
    )
  }
  override def callableProcedure: Some[CallableProcedure] = {
    procedure
  }
}
