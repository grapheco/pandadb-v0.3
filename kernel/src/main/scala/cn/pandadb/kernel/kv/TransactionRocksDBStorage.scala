package cn.pandadb.kernel.kv

import java.io.File

import cn.pandadb.kernel.kv.configuration.RocksDBConfigBuilder
import cn.pandadb.kernel.kv.db.{KeyValueIterator, KeyValueTransactionDB}
import cn.pandadb.kernel.util.PandaDBException.PandaDBException
import com.typesafe.scalalogging.LazyLogging
import org.rocksdb._

/**
 * @program: pandadb-v0.3
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-08-04 10:29
 */
object TransactionRocksDBStorage extends LazyLogging {
  RocksDB.loadLibrary()
  val top = new TransactionDBOptions()

  def getDB(path: String,
            createIfMissing: Boolean = true,
            withBloomFilter: Boolean = true,
            isHDD: Boolean = false,
            useForImporter: Boolean = false,
            prefix: Int = 0,
            rocksdbConfigPath: String = "default"): TransactionDB = {

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

      tableConfig.setFilterPolicy(new BloomFilter(15, false))
        .setBlockSize(16L * 1024L)
        .setBlockCache(new LRUCache(1024L * 1024L * 1024L))

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
        .setMaxBytesForLevelBase(256L * 1024L * 1024L * 2 * 10L) // total size of level1(same as level0)
        .setMaxBytesForLevelMultiplier(10)
        .setTargetFileSizeBase(256L * 1024L * 1024L) // maxBytesForLevelBase / 10 or 15
        .setTargetFileSizeMultiplier(2)

      try {
         TransactionDB.open(options, top, path)
      } catch {
        case ex: Exception => throw new PandaDBException(s"$path, ${ex.getMessage}")
      }

    } else {
      logger.debug("read setting file")
      val rocksFile = new File(rocksdbConfigPath)
      if (!rocksFile.exists()) throw new PandaDBException("rocksdb config file not exist...")
      val options = new RocksDBConfigBuilder(rocksFile).getOptions()
      TransactionDB.open(options, top, path)
    }
  }
}