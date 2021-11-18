package cn.pandadb.kernel.distribute.meta

import org.tikv.common.types.Charset

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 16:53
 */
object MetaNameMapping {
  // doc column name
  val metaName = "name"
  val metaId = "id"

  // index name
  val nodeMetaName = "node-meta"
  val relationMetaName = "relation-meta"
  val nodePropertyMetaName = "node-property-meta"
  val relationPropertyMetaName = "relation-property-meta"

  // id index name
  val idGeneratorName = "id-meta"
}
