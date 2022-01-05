package cn.pandadb.kernel.distribute.meta
import cn.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import cn.pandadb.kernel.udp.UDPClient

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 16:52
 */
class NodeLabelNameStore(_db: DistributedKVAPI, _udpClients: Array[UDPClient]) extends DistributedNameStore {
    override val initInt: Int = 0
    override val db: DistributedKVAPI = _db
    override val key2ByteArrayFunc: Int => Array[Byte] = DistributedKeyConverter.nodeLabelKeyToBytes
    override val keyPrefixFunc: () => Array[Byte] = DistributedKeyConverter.nodeLabelKeyPrefixToBytes
    override val udpClients: Array[UDPClient] = _udpClients
    loadAll()
}
