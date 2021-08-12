package cn.pandadb.kernel.transaction

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 5:01 下午 2021/8/6
 * @Modified By:
 */
trait TransactionManager {

  def begin(): PandaTransaction


}
