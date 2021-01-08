package cn.pandadb.kernel.kv

import java.io.File

import org.rocksdb.{BlockBasedTableConfig, BloomFilter, CompactionStyle, CompressionType, IndexType, LRUCache, Options, RocksDB}

object RocksDBStorage {
  RocksDB.loadLibrary()

  def getDB(path:String, createIfMissing:Boolean=true): RocksDB = {
    val options: Options = new Options().setCreateIfMissing(createIfMissing)
    val blockConfig = new BlockBasedTableConfig()
    val bloomFilter = new BloomFilter(15, false)
    blockConfig.setFilterPolicy(bloomFilter)

//    blockConfig.setCacheIndexAndFilterBlocks(true)
    options.setTableFormatConfig(blockConfig)
    options.setIncreaseParallelism(Runtime.getRuntime.availableProcessors() * 2)


    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    if (dir.exists && !dir.isDirectory)
      throw new IllegalStateException("Invalid db path, it's a regular file: " + path)

    RocksDB.open(options, path)
  }


  def getDB2(path:String, createIfMissing:Boolean=true): RocksDB = {
    val options: Options = new Options()
      .setCreateIfMissing(createIfMissing)
      .useCappedPrefixExtractor(10)

    val table_option = new BlockBasedTableConfig()
    table_option.setIndexType(IndexType.kHashSearch)
    table_option.setBlockSize(4 * 1024)
    options.setTableFormatConfig(table_option)
      .setCompressionType(CompressionType.LZ4_COMPRESSION)
      .setMaxOpenFiles(-1)
      .setCompactionStyle(CompactionStyle.LEVEL)
      .setLevel0FileNumCompactionTrigger(10)
      .setLevel0SlowdownWritesTrigger(20)
      .setLevel0StopWritesTrigger(40)
      .setWriteBufferSize(64 * 1024 * 1024)
      .setTargetFileSizeBase(64 * 1024 * 1024)
      .setMaxBytesForLevelBase(512 * 1024 * 1024)
      .setMaxBackgroundCompactions(1)
      .setMaxBackgroundFlushes(1)
//      .setMemtablePrefixBloomSizeRatio()
//
    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    if (dir.exists && !dir.isDirectory)
      throw new IllegalStateException("Invalid db path, it's a regular file: " + path)

    RocksDB.open(options, path)
  }

  def getInitDB(path:String = "testdata/rocks/db", createIfMissing:Boolean=true): RocksDB = {
    val options: Options = new Options().setCreateIfMissing(createIfMissing)
      .setAllowConcurrentMemtableWrite(true).setMaxBackgroundFlushes(1)
      .setWriteBufferSize(256*1024*1024).setMaxWriteBufferNumber(8).setMinWriteBufferNumberToMerge(4)
      .setArenaBlockSize(512*1024)
      .setLevel0FileNumCompactionTrigger(512).setDisableAutoCompactions(true)
      .setMaxBackgroundCompactions(8)

    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    if (dir.exists && !dir.isDirectory)
      throw new IllegalStateException("Invalid db path, it's a regular file: " + path)

    RocksDB.open(options, path)
  }
}

class RocksDBStorage(path: String) {
  val db: RocksDB = RocksDBStorage.getDB(path)
  def close(): Unit = ???

}

