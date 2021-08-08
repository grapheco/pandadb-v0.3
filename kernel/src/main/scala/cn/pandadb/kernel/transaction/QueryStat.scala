package cn.pandadb.kernel.transaction

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 8:19 下午 2021/8/8
 * @Modified By:
 */
case class QueryStat(statment: String, var status: QUERYSTATUS.Value) {
  def toLogString: String = {
    s"$statment #$status"
  }
}
