package org.grapheco.pandadb.kernel.distribute.index

import org.grapheco.pandadb.kernel.distribute.meta.{NodeLabelNameStore, PropertyNameStore}
import org.grapheco.pandadb.kernel.distribute.node.DistributedNodeStoreSPI
import org.grapheco.pandadb.kernel.distribute.{DistributedGraphService, DistributedKVAPI, DistributedKeyConverter}
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-22 14:28
 */
class NodeIndexMetaStore(_db: DistributedKVAPI, _graphService: DistributedGraphService, _udpClientManager: UDPClientManager) extends IndexNameStore {
  override val db: DistributedKVAPI = _db
  override val keyPrefixFunc: () => Array[Byte] = DistributedKeyConverter.indexMetaPrefixToBytes
  override val encodingKeyPrefix: () => Array[Byte] = ()=>Array(DistributedKeyConverter.indexEncoderPrefix)
  override val keyWithLabelPrefixFunc: Int => Array[Byte] = DistributedKeyConverter.indexMetaWithLabelPrefixToBytes
  override val keyWithIndexFunc: (Int, Int) => Array[Byte] = DistributedKeyConverter.indexMetaToBytes
  override val graphService: DistributedGraphService = _graphService
  override val udpClientManager: UDPClientManager = _udpClientManager
  loadAll()
}
