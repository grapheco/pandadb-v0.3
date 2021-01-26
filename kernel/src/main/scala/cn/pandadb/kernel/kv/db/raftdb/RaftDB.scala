package cn.pandadb.kernel.kv.db.raftdb

import cn.pandadb.kernel.kv.db.{KeyValueDB, KeyValueIterator}
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore

import java.util.concurrent.CompletableFuture


class RaftDB(DB_PATH: String,
             RAFT_DATA_PATH: String,
             ALL_NODE_ADDRESSES: String,
             CLUSTER_NAME: String
            ) extends KeyValueDB{

  private val store = new DefaultRheaKVStore

  import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions
  import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions
  import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions
  import com.alipay.sofa.jraft.rhea.options.configured.MultiRegionRouteTableOptionsConfigured
  import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured
  import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured
  import java.util

  val regionRouteTableOptionsList: util.List[RegionRouteTableOptions] = MultiRegionRouteTableOptionsConfigured.newConfigured.withInitialServerList(-(1L), ALL_NODE_ADDRESSES).config
  val pdOpts: PlacementDriverOptions = PlacementDriverOptionsConfigured.newConfigured.withFake(true).withRegionRouteTableOptionsList(regionRouteTableOptionsList).config
  val opts: RheaKVStoreOptions = RheaKVStoreOptionsConfigured.newConfigured.withClusterName(CLUSTER_NAME).withPlacementDriverOptions(pdOpts).config
  System.out.println(opts)

  store.init(opts)

  var cnt: Int = 0

  override def get(key: Array[Byte]): Array[Byte] = store.bGet(key)

  def aGet(key: Array[Byte]): CompletableFuture[Array[Byte]] = {
    store.get(key)
  }

  override def put(key: Array[Byte], value: Array[Byte]): Unit = {
    store.bPut(key, value)
  }

  def aPut(key: Array[Byte], value: Array[Byte]): CompletableFuture[java.lang.Boolean] = {
    store.put(key, value)
  }

  override def write(option: Any, batch: Any): Unit = ???

  override def delete(key: Array[Byte]): Unit = store.bDelete(key)

  override def deleteRange(key1: Array[Byte], key2: Array[Byte]): Unit = store.bDeleteRange(key1, key2)

  override def newIterator(): KeyValueIterator = new RaftKVIterator(store)

  override def flush(): Unit = {}

  override def close(): Unit = store.shutdown()
}

