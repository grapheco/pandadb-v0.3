package cn.pandadb.kernel.kv.lynx.procedure.functions

import java.text.SimpleDateFormat

import cn.pandadb.kernel.kv.lynx.procedure.PandaFunction
import org.grapheco.lynx.{CallableProcedure, LynxDate, LynxInteger, LynxString, LynxType, LynxValue, LynxRelationship}
import org.opencypher.v9_0.util.symbols.CTRelationship

case object TypeFunction extends PandaFunction {
  override def name: String = ".type"
  val procedure = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("relationshipType"->CTRelationship)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          val value = args.head
          value match {
            case relationship: LynxRelationship =>
              Iterable(Seq(LynxString(relationship.relationType.get)))
            case other => throw new FunctionTypeException(other.cypherType.toString)
          }
        }
      }
    )
  }
  override def callableProcedure: Some[CallableProcedure] = {
    procedure
  }
}

class FunctionTypeException(msg: String) extends Exception{
  override def getMessage: String = s"expect relationship but was $msg"
}