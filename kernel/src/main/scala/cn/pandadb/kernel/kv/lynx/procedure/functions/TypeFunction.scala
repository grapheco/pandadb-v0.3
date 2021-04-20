package cn.pandadb.kernel.kv.lynx.procedure.functions

import java.text.SimpleDateFormat

import cn.pandadb.kernel.kv.lynx.procedure.PandaFunction
import org.grapheco.lynx.{CallableProcedure, LynxDate, LynxInteger, LynxString, LynxType, LynxValue, LynxRelationship}
import org.opencypher.v9_0.util.symbols.CTRelationship

case object TypeFunction extends PandaFunction{
  override def name: String = ".type"
  val procedure = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("relationshipType"->CTRelationship)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          Iterable(Seq(LynxString(args.head.asInstanceOf[LynxRelationship].relationType.get)))
        }
      }
    )
  }
  override def callableProcedure: Some[CallableProcedure] = {
    procedure
  }
}
