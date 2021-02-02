package cn.pandadb.kernel.kv

import java.io.File

import cn.pandadb.kernel.kv.configuration.RocksDBConfigBuilder
import cn.pandadb.kernel.kv.db.{KeyValueDB, KeyValueIterator, KeyValueWriteBatch}
import org.rocksdb.{BlockBasedTableConfig, BloomFilter, CompactionStyle, CompressionType, FlushOptions, IndexType, LRUCache, Options, RocksDB, WriteBatch, WriteOptions}

object RocksDBStorage {
  RocksDB.loadLibrary()

  def getDB(path: String,
            createIfMissing: Boolean = true,
            withBloomFilter: Boolean = true,
            isHDD: Boolean = false,
            useForImporter: Boolean = false,
            prefix: Int = 0): KeyValueDB = {

    val options: Options = new Options()
    val tableConfig = new BlockBasedTableConfig()

    // bloom filter
    if (withBloomFilter) {
      tableConfig.setFilterPolicy(new BloomFilter(15, false))
    }
    // prefixExtractor
    if (prefix > 0) {
      options.useCappedPrefixExtractor(prefix)
      tableConfig.setIndexType(IndexType.kHashSearch)
    }
    // ssd or hdd
    if (isHDD) {
      tableConfig.setBlockSize(256 * 1024)
      //      tableConfig.setCacheIndexAndFilterBlocks(true)

      options.setOptimizeFiltersForHits(true)
        .setSkipCheckingSstFileSizesOnDbOpen(true)
        .setLevelCompactionDynamicLevelBytes(true)
        .setWriteBufferSize(256 * 1024 * 1024)
        .setTargetFileSizeBase(256 * 1024 * 1024)
      //      .setMaxFileOpeningThreads(1) // if multi hard disk, set this
    } else {
      tableConfig.setBlockSize(4 * 1024)
      options.setWriteBufferSize(64 * 1024 * 1024)
        .setTargetFileSizeBase(64 * 1024 * 1024)
    }

    options.setTableFormatConfig(tableConfig)
      .setCreateIfMissing(createIfMissing)
      .setCompressionType(CompressionType.LZ4_COMPRESSION)
      .setMaxOpenFiles(-1)
      .setCompactionStyle(CompactionStyle.LEVEL)
      .setDisableAutoCompactions(true)
      .setLevel0FileNumCompactionTrigger(10)
      .setLevel0SlowdownWritesTrigger(20)
      .setLevel0StopWritesTrigger(40)
      .setMaxBytesForLevelBase(512 * 1024 * 1024)

    if (useForImporter) {
      options.setAllowConcurrentMemtableWrite(true)
        .setWriteBufferSize(256 * 1024 * 1024)
        .setMaxWriteBufferNumber(8)
        .setMinWriteBufferNumberToMerge(4)
        .setArenaBlockSize(512 * 1024)
        .setLevel0FileNumCompactionTrigger(512)
        .setDisableAutoCompactions(true)
      //        .setMaxBackgroundCompactions(8)
    }

    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    if (dir.exists && !dir.isDirectory)
      throw new IllegalStateException("Invalid db path, it's a regular file: " + path)

    new RocksDBStorage(RocksDB.open(options, path))
  }
}

class RocksDBStorage(val rocksDB: RocksDB) extends KeyValueDB {

  override def get(key: Array[Byte]): Array[Byte] = rocksDB.get(key)

  override def put(key: Array[Byte], value: Array[Byte]): Unit = rocksDB.put(key, value)

  override def write(option: Any, batch: Any): Unit = rocksDB.write(option.asInstanceOf[WriteOptions], batch.asInstanceOf[WriteBatch])

  override def delete(key: Array[Byte]): Unit = rocksDB.delete(key)

  override def deleteRange(key1: Array[Byte], key2: Array[Byte]): Unit = rocksDB.deleteRange(key1, key2)

  override def newIterator(): KeyValueIterator = {
    val iter = rocksDB.newIterator()
    new KeyValueIterator {
      override def isValid: Boolean = iter.isValid

      override def seek(key: Array[Byte]): Unit = iter.seek(key)

      override def seekToFirst(): Unit = iter.seekToFirst()

      override def seekToLast(): Unit = iter.seekToLast()

      override def seekForPrev(key: Array[Byte]): Unit = iter.seekForPrev(key)

      override def next(): Unit = iter.next()

      override def prev(): Unit = iter.prev()

      override def key(): Array[Byte] = iter.key()

      override def value(): Array[Byte] = iter.value()
    }
  }

  override def flush(): Unit = rocksDB.flush(new FlushOptions)

  override def close(): Unit = rocksDB.close()
}

