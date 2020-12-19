package cn.pandadb.kernel.util.serializer

import io.netty.buffer.ByteBufAllocator

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 17:19 2020/12/18
 * @Modified By:
 */
object EdgeSerializer extends BaseSerializer {
  override val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT


}
