package cn.pandadb.kernel.kv.lynx.procedure.functions

import cn.pandadb.kernel.kv.lynx.procedure.PandaFunction
import org.grapheco.lynx.{CallableProcedure, LynxBoolean, LynxInteger, LynxNode, LynxRelationship, LynxType, LynxValue}
import org.opencypher.v9_0.util.symbols.CTInteger

case object IdFunction extends PandaFunction{
  override def name: String = ".id"
  val procedure = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("id"->CTInteger)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          Iterable(Seq(LynxInteger(args match {
            case sNode: Seq[LynxNode] => sNode.head.id.value.asInstanceOf[Long]
            case sRel: Seq[LynxRelationship] => sRel.head.id.value.asInstanceOf[Long]
          })))
        }
      }
    )
  }
  override def callableProcedure: Some[CallableProcedure] = {
    procedure
  }
}
