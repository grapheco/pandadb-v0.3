package cn.pandadb.kernel.kv

import java.io.File

import org.rocksdb.{BlockBasedTableConfig, BloomFilter, CompactionStyle, CompressionType, IndexType, LRUCache, Options, RocksDB}

object RocksDBStorage {
  RocksDB.loadLibrary()


  def getDB(path:String,
            createIfMissing:Boolean = true,
            withBloomFilter:Boolean = true,
            isHDD: Boolean = true,
            useForImporter: Boolean = false,
            prefix: Int = 0): RocksDB = {
    val options: Options = new Options()
    val tableConfig = new BlockBasedTableConfig()

    // bloom filter
    if(withBloomFilter){
      tableConfig.setFilterPolicy(new BloomFilter(15, false))
    }
    // prefixExtractor
    if (prefix > 0){
      options.useCappedPrefixExtractor(prefix)
      tableConfig.setIndexType(IndexType.kHashSearch)
    }
    // ssd or hdd
    if(isHDD) {
      tableConfig.setBlockSize(256 * 1024)
//      tableConfig.setCacheIndexAndFilterBlocks(true)
      options.setOptimizeFiltersForHits(true)
      .setSkipCheckingSstFileSizesOnDbOpen(true)
      .setLevelCompactionDynamicLevelBytes(true)
      .setWriteBufferSize(256 * 1024 * 1024)
      .setTargetFileSizeBase(256 * 1024 * 1024)
//      .setMaxFileOpeningThreads(1) // if multi hard disk, set this
    }else {
      tableConfig.setBlockSize(4 * 1024)
      options.setWriteBufferSize(64 * 1024 * 1024)
        .setTargetFileSizeBase(64 * 1024 * 1024)
    }

    options.setTableFormatConfig(tableConfig)
      .setCreateIfMissing(createIfMissing)
      .setCompressionType(CompressionType.LZ4_COMPRESSION)
      .setMaxOpenFiles(-1)
      .setCompactionStyle(CompactionStyle.LEVEL)
      .setLevel0FileNumCompactionTrigger(10)
      .setLevel0SlowdownWritesTrigger(20)
      .setLevel0StopWritesTrigger(40)
      .setMaxBytesForLevelBase(512 * 1024 * 1024)

    if (useForImporter){
      options.setAllowConcurrentMemtableWrite(true)
        .setMaxBackgroundFlushes(1)
        .setWriteBufferSize(256*1024*1024)
        .setMaxWriteBufferNumber(8)
        .setMinWriteBufferNumberToMerge(4)
        .setArenaBlockSize(512*1024)
        .setLevel0FileNumCompactionTrigger(512)
        .setDisableAutoCompactions(true)
        .setMaxBackgroundCompactions(8)
    }

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

