package org.grapheco.pandadb.kernel.distribute.meta

import org.grapheco.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import org.grapheco.pandadb.kernel.udp.UDPClientManager

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 16:52
 */
class RelationTypeNameStore(_db: DistributedKVAPI, _udpClientManager: UDPClientManager) extends DistributedNameStore {
  override val initInt: Int = 0
  override val db: DistributedKVAPI = _db
  override val key2ByteArrayFunc: Int => Array[Byte] = DistributedKeyConverter.relationTypeKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = DistributedKeyConverter.relationTypeKeyPrefixToBytes
  override val udpClientManager: UDPClientManager = _udpClientManager
  loadAll()
}
