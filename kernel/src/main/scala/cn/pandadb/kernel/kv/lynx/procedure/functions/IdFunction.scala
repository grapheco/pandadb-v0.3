package cn.pandadb.kernel.kv.lynx.procedure.functions

import cn.pandadb.kernel.kv.lynx.procedure.PandaFunction
import cn.pandadb.kernel.store.{PandaNode, PandaRelationship}
import org.grapheco.lynx.{CallableProcedure, LynxBoolean, LynxInteger, LynxNode, LynxNull, LynxRelationship, LynxType, LynxValue}
import org.opencypher.v9_0.util.symbols.CTInteger

case object IdFunction extends PandaFunction{
  override def name: String = ".id"
  val procedure = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("id"->CTInteger)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          Iterable(Seq(LynxInteger(args.head match {
            case sNode: LynxNode => sNode.id.value.asInstanceOf[Int]
            case sRel: LynxRelationship => sRel.id.value.asInstanceOf[Int]
          })))
        }
      }
    )
  }
  override def callableProcedure: Some[CallableProcedure] = {
    procedure
  }
}
