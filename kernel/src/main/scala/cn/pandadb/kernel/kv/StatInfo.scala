package cn.pandadb.kernel.kv

import org.rocksdb.RocksDB



trait MetaInfo {
  def getLabelsCount
  def getRelLabelsCount
  //def
}

class StatInfo(rocksDB: RocksDBGraphAPI) {

}
