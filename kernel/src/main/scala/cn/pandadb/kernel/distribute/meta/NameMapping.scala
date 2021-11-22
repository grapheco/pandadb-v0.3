package cn.pandadb.kernel.distribute.meta

import org.tikv.common.types.Charset

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 16:53
 */
object NameMapping {
  // doc column name
  val metaName = "name"
  val metaId = "id"

  // index name
  val nodeLabelMetaName = "node-label-meta"
  val relationTypeMetaName = "relation-type-meta"
  val nodePropertyMetaName = "node-property-meta"
  val relationPropertyMetaName = "relation-property-meta"

  // id index name
  val idGeneratorName = "id-meta"

  // data index meta
  val nodeIndex = "node-index"
  val relationIndex = "relation-index"
  val nodeIndexMeta = "node-index-meta"
  val relationIndexMeta = "relation-index-meta"
}
