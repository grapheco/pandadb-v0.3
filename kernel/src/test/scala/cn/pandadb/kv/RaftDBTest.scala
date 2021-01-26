package cn.pandadb.kv

import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured
import com.alipay.sofa.jraft.rhea.options.configured.RocksDBOptionsConfigured
import com.alipay.sofa.jraft.rhea.options.configured.StoreEngineOptionsConfigured
import com.alipay.sofa.jraft.rhea.storage.StorageType
import com.alipay.sofa.jraft.util.Endpoint

import java.io.File
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore
import com.alipay.sofa.jraft.rhea.client.RheaKVStore
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions


class Node(val options: RheaKVStoreOptions) {
  private var rheaKVStore: RheaKVStore = null

  def start(): Unit = {
    this.rheaKVStore = new DefaultRheaKVStore
    this.rheaKVStore.init(options)
  }

  def stop(): Unit = {
    this.rheaKVStore.shutdown()
  }

  def getRheaKVStore: RheaKVStore = rheaKVStore
}


object Configs {
  var DB_PATH: String = "D:\\PandaDB-tmp\\rhea_db" + File.separator
  var RAFT_DATA_PATH: String = "D:\\PandaDB-tmp\\raft_data" + File.separator
  var ALL_NODE_ADDRESSES = "127.0.0.1:8181,127.0.0.1:8182,127.0.0.1:8183"
  var CLUSTER_NAME = "rhea_example"
}

object Server1 {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val pdOpts = PlacementDriverOptionsConfigured.newConfigured.withFake(true).config // use a fake pd

    val storeOpts = StoreEngineOptionsConfigured.newConfigured.withStorageType(StorageType.RocksDB).
      withRocksDBOptions(RocksDBOptionsConfigured.newConfigured.withDbPath(Configs.DB_PATH).config).
      withRaftDataPath(Configs.RAFT_DATA_PATH).
      withServerAddress(new Endpoint("127.0.0.1", 8181)).config
    val opts = RheaKVStoreOptionsConfigured.newConfigured.withClusterName(Configs.CLUSTER_NAME).
      withInitialServerList(Configs.ALL_NODE_ADDRESSES).
      withStoreEngineOptions(storeOpts).withPlacementDriverOptions(pdOpts).config
    System.out.println(opts)
    val node = new Node(opts)
    node.start
    sys.addShutdownHook(node.stop)
    System.out.println("server1 start OK")
  }
}

object Server2 {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val pdOpts = PlacementDriverOptionsConfigured.newConfigured.withFake(true).config // use a fake pd

    val storeOpts = StoreEngineOptionsConfigured.newConfigured.withStorageType(StorageType.RocksDB).
      withRocksDBOptions(RocksDBOptionsConfigured.newConfigured.withDbPath(Configs.DB_PATH).config).
      withRaftDataPath(Configs.RAFT_DATA_PATH).
      withServerAddress(new Endpoint("127.0.0.1", 8182)).config
    val opts = RheaKVStoreOptionsConfigured.newConfigured.withClusterName(Configs.CLUSTER_NAME).
      withInitialServerList(Configs.ALL_NODE_ADDRESSES).
      withStoreEngineOptions(storeOpts).withPlacementDriverOptions(pdOpts).config
    System.out.println(opts)
    val node = new Node(opts)
    node.start
    sys.addShutdownHook(node.stop)
    System.out.println("server1 start OK")
  }
}

object Server3 {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val pdOpts = PlacementDriverOptionsConfigured.newConfigured.withFake(true).config // use a fake pd

    val storeOpts = StoreEngineOptionsConfigured.newConfigured.withStorageType(StorageType.RocksDB).
      withRocksDBOptions(RocksDBOptionsConfigured.newConfigured.withDbPath(Configs.DB_PATH).config).
      withRaftDataPath(Configs.RAFT_DATA_PATH).
      withServerAddress(new Endpoint("127.0.0.1", 8183)).config
    val opts = RheaKVStoreOptionsConfigured.newConfigured.withClusterName(Configs.CLUSTER_NAME).
      withInitialServerList(Configs.ALL_NODE_ADDRESSES).
      withStoreEngineOptions(storeOpts).withPlacementDriverOptions(pdOpts).config
    System.out.println(opts)
    val node = new Node(opts)
    node.start
    sys.addShutdownHook(node.stop)
    System.out.println("server1 start OK")
  }
}
