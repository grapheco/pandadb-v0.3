package cn.pandadb.kernel.kv

import java.io.File

import cn.pandadb.kernel.kv.configuration.RocksDBConfigBuilder
import cn.pandadb.kernel.kv.db.{KeyValueDB, KeyValueIterator, KeyValueWriteBatch}
import cn.pandadb.kernel.util.PandaDBException.PandaDBException
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
      logger.debug("use default setting")
      val options: Options = new Options()
      val tableConfig = new BlockBasedTableConfig()

      tableConfig.setFilterPolicy(new BloomFilter(10, false))
        .setBlockSize(512L * 1024L)
        .setBlockCache(new LRUCache(1024L * 1024L * 1024L))

      println("block size: ", tableConfig.blockSize())

      options.setTableFormatConfig(tableConfig)
        .setCreateIfMissing(createIfMissing)
        .setCompressionType(CompressionType.LZ4_COMPRESSION)
        .setCompactionStyle(CompactionStyle.LEVEL)
        .setDisableAutoCompactions(false) // true will invalid the compaction trigger, maybe
        .setOptimizeFiltersForHits(false) // true will not generate BloomFilter for L0.
        .setMaxBackgroundJobs(4)
        .setMaxWriteBufferNumber(8)
        .setSkipCheckingSstFileSizesOnDbOpen(true)
        .setLevelCompactionDynamicLevelBytes(true)
        .setAllowConcurrentMemtableWrite(true)
        .setCompactionReadaheadSize(2 * 1024 * 1024L)
        .setMaxOpenFiles(-1)
        .setUseDirectIoForFlushAndCompaction(true) // maybe importer use
        .setWriteBufferSize(256L * 1024L * 1024L)
        .setMinWriteBufferNumberToMerge(2) // 2 or 3 better performance
        .setLevel0FileNumCompactionTrigger(10) // level0 file num = 10, compression l0->l1 start.(invalid, why???);level0 size=256M * 3 * 10 = 7G
        .setLevel0SlowdownWritesTrigger(20)
        .setLevel0StopWritesTrigger(40)
        .setMaxBytesForLevelBase(256L * 1024L * 1024L * 15L) // total size of level1(same as level0)
        .setMaxBytesForLevelMultiplier(10)
        .setTargetFileSizeBase(256L * 1024L * 1024L) // maxBytesForLevelBase / 10 or 15
          .setTargetFileSizeMultiplier(2)

      logger.debug(s"setDisableAutoCompactions: ${options.disableAutoCompactions()}")
      logger.debug(s"setWriteBufferSize: ${options.writeBufferSize()}")
      logger.debug(s"setMaxBytesForLevelBase: ${options.maxBytesForLevelBase()}")

      try {
        new RocksDBStorage(RocksDB.open(options, path))
      } catch {
        case ex: Exception => throw new PandaDBException(s"$path, ${ex.getMessage}")
      }

    } else {
      logger.debug("read setting file")
      val rocksFile = new File(rocksdbConfigPath)
      if (!rocksFile.exists()) throw new PandaDBException("rocksdb config file not exist...")
      val builder = new RocksDBConfigBuilder(rocksFile)
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

