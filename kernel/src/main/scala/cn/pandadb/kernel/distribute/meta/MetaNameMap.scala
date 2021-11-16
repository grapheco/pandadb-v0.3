package cn.pandadb.kernel.distribute.meta

import org.tikv.common.types.Charset

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 16:53
 */
object MetaNameMap {
  // doc column name
  val metaName = "name"
  val metaId = "id"

  // index name
  val nodeMetaName = "node-meta"
  val relationMetaName = "relation-meta"
  val propertyMetaName = "property-meta"
}
