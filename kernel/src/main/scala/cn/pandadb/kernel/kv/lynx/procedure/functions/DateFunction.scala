package cn.pandadb.kernel.kv.lynx.procedure.functions

import java.text.SimpleDateFormat
import java.util.Date

import cn.pandadb.kernel.kv.lynx.procedure.PandaFunction
import cn.pandadb.kernel.kv.value.{LynxDateTimeUtil, LynxDateUtil, LynxLocalDateTimeUtil, LynxLocalTimeUtil, LynxTimeUtil}
import org.grapheco.lynx.{CallableProcedure, LynxDate, LynxInteger, LynxString, LynxType, LynxValue}
import org.opencypher.v9_0.util.symbols.{CTDate, CTDateTime, CTLocalDateTime, CTLocalTime, CTTime}

case object DateFunction extends PandaFunction{
  override def name: String = ".date"
  val procedure = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("date"->CTDate)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          //TODO: support like yyyy/MM/dd HH:mm:ss
          if(args.size == 0) Iterable(Seq(LynxDateUtil.now()))
          else Iterable(Seq(LynxDateUtil.parse(args.head.value.toString)))

        }
      }
    )
  }

  override def callableProcedure: Some[CallableProcedure] = {
    procedure
  }
}


case object DateTimeFunction extends PandaFunction{
  override def name: String = ".datetime"
  val procedure = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("datetime"->CTDateTime)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          if(args.size == 0) Iterable(Seq(LynxDateTimeUtil.now()))
          else Iterable(Seq(LynxDateTimeUtil.parse(args.head.value.toString)))
        }
      }
    )
  }
  override def callableProcedure: Some[CallableProcedure] = {
    procedure
  }
}

case object LocalDateTimeFunction extends PandaFunction{
  override def name: String = ".localdatetime"
  val procedure = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("localdatetime"->CTLocalDateTime)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          if(args.size == 0) Iterable(Seq(LynxLocalDateTimeUtil.now()))
          else Iterable(Seq(LynxLocalDateTimeUtil.parse(args.head.value.toString)))
        }
      }
    )
  }
  override def callableProcedure: Some[CallableProcedure] = {
    procedure
  }
}

case object TimeFunction extends PandaFunction{
  override def name: String = ".time"
  val procedure = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("time"->CTLocalTime)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          if(args.size == 0) Iterable(Seq(LynxTimeUtil.now()))
          else Iterable(Seq(LynxTimeUtil.parse(args.head.value.toString)))
        }
      }
    )
  }
  override def callableProcedure: Some[CallableProcedure] = {
    procedure
  }
}

case object LocalTimeFunction extends PandaFunction{
  override def name: String = ".localtime"
  val procedure = {
    Some(
      new CallableProcedure {
        override val inputs: Seq[(String, LynxType)] = Seq()
        override val outputs: Seq[(String, LynxType)] = Seq("localtime"->CTLocalTime)

        override def call(args: Seq[LynxValue]): Iterable[Seq[LynxValue]] = {
          if(args.size == 0) Iterable(Seq(LynxLocalTimeUtil.now()))
          else Iterable(Seq(LynxLocalTimeUtil.parse(args.head.value.toString)))
        }
      }
    )
  }
  override def callableProcedure: Some[CallableProcedure] = {
    procedure
  }
}
