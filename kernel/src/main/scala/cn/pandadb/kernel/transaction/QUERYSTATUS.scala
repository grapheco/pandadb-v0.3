package cn.pandadb.kernel.transaction

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 7:15 下午 2021/8/8
 * @Modified By:
 */
object QUERYSTATUS extends Enumeration {
  val SUCCEED = Value(1, "succeed")
  val EXECUTING = Value(2, "executing")
  val FAILED = Value(3, "failed")
}