package cn.pandadb.kernel.kv

import java.io.File

import cn.pandadb.kernel.kv.configuration.RocksDBConfigBuilder
import cn.pandadb.kernel.kv.db.{KeyValueDB, KeyValueIterator, KeyValueWriteBatch}
import com.typesafe.scalalogging.LazyLogging
import org.rocksdb.{BlockBasedTableConfig, BloomFilter, CompactionStyle, CompressionType, FlushOptions, IndexType, LRUCache, Options, ReadOptions, RocksDB, WriteBatch, WriteOptions}

object RocksDBStorage extends LazyLogging{
  RocksDB.loadLibrary()

  def getDB(path: String,
            createIfMissing: Boolean = true,
            withBloomFilter: Boolean = true,
            isHDD: Boolean = false,
            useForImporter: Boolean = false,
            prefix: Int = 0,
            rocksdbConfigPath: String = "default"): KeyValueDB = {

    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    if (dir.exists && !dir.isDirectory)
      throw new IllegalStateException("Invalid db path, it's a regular file: " + path)

    if (rocksdbConfigPath == "default") {
      logger.info("go default setting~~~~~~~~~~")
      val options: Options = new Options()
      val tableConfig = new BlockBasedTableConfig()

      tableConfig.setFilterPolicy(new BloomFilter(15, false))
        .setBlockSize(16 * 1024)
        .setBlockCache(new LRUCache(256 * 1024 * 1024))
        .setFormatVersion(4)
        .setWholeKeyFiltering(false)

      options.setTableFormatConfig(tableConfig)
        .setCreateIfMissing(createIfMissing)
        .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
        .setCompactionStyle(CompactionStyle.LEVEL)
        .setDisableAutoCompactions(true) // true will invalid the compaction trigger, maybe
        .setOptimizeFiltersForHits(true) // true will not generate BloomFilter for L0. dangerous
        .setSkipCheckingSstFileSizesOnDbOpen(true)
        .setLevelCompactionDynamicLevelBytes(true)
        .setAllowConcurrentMemtableWrite(true)
        .setMaxOpenFiles(-1)
        .setWriteBufferSize(256 * 1024 * 1024)
        .setMinWriteBufferNumberToMerge(4)
        .setLevel0FileNumCompactionTrigger(10) // level0 file num = 10, compression l0->l1 start.(invalid, why???);level0 size=1G * 4 * 10 = 40
        .setMaxWriteBufferNumber(8)
        .setLevel0SlowdownWritesTrigger(20)
        .setLevel0StopWritesTrigger(40)
        .setMaxBytesForLevelBase(1024 * 1024 * 1024 * 40) // total size of level1(same as level0)
        .setTargetFileSizeBase(1024 * 1024 * 1024 * 4) // maxBytesForLevelBase / 10
        .setArenaBlockSize(512 * 1024 * 1024)

      new RocksDBStorage(RocksDB.open(options, path))

    } else {
      logger.info("go setting file ~~~~~~~~~")
      val builder = new RocksDBConfigBuilder(rocksdbConfigPath)
      val db = RocksDB.open(builder.getOptions(), path)
      new RocksDBStorage(db)
    }
  }
}

class RocksDBStorage(val rocksDB: RocksDB) extends KeyValueDB {

  override def get(key: Array[Byte]): Array[Byte] = {
    rocksDB.get(key)
  }

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

