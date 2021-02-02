package cn.pandadb.kernel.kv.configuration

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.Properties

import org.rocksdb.{BlockBasedTableConfig, BloomFilter, CompactionStyle, CompressionType, IndexType, Options}

class RocksDBConfigBuilder(rocksdbFile: File) {
  val options: Options = new Options()
  val tableConfig = new BlockBasedTableConfig()

  val prop = new Properties()
  val is = new BufferedInputStream(new FileInputStream(rocksdbFile))
  prop.load(is)

  def getOptions(): Options ={
    
    val isHDD = prop.getProperty("rocksdb.isHDD").toBoolean
    val isUseWithBloomFilter = prop.getProperty("rocksdb.withBloomFilter").toBoolean
    val createIfMissing = prop.getProperty("rocksdb.createIfMissing").toBoolean
    val useForImporter = prop.getProperty("rocksdb.useForImporter").toBoolean
    val prefix = prop.getProperty("rocksdb.prefix").toInt

    if (isUseWithBloomFilter) {
      tableConfig.setFilterPolicy(new BloomFilter(prop.getProperty("rocksdb.BloomFilter_1").toDouble, prop.getProperty("rocksdb.BloomFilter_2").toBoolean))
    }
    if (prefix > 0) {
      options.useCappedPrefixExtractor(prefix)
      val indexType = {
        prop.getProperty("rocksdb.BlockBasedTableConfig.setIndexType") match {
          case "IndexType.kHashSearch" => IndexType.kHashSearch
          case "IndexType.kBinarySearch" => IndexType.kBinarySearch
          case "IndexType.kBinarySearchWithFirstKey" => IndexType.kBinarySearchWithFirstKey
          case "IndexType.kTwoLevelIndexSearch" => IndexType.kTwoLevelIndexSearch
        }
      }
      tableConfig.setIndexType(indexType)
    }

    if (isHDD) {
      tableConfig.setBlockSize(prop.getProperty("rocksdb.BlockBasedTableConfig.setBlockSize").toLong)
      options.setOptimizeFiltersForHits(prop.getProperty("rocksdb.Options.setOptimizeFiltersForHits").toBoolean)
      options.setSkipCheckingSstFileSizesOnDbOpen(prop.getProperty("rocksdb.Options.setSkipCheckingSstFileSizesOnDbOpen").toBoolean)
      options.setLevelCompactionDynamicLevelBytes(prop.getProperty("rocksdb.Options.setLevelCompactionDynamicLevelBytes").toBoolean)
      options.setWriteBufferSize(prop.getProperty("rocksdb.Options.setWriteBufferSize").toLong)
      options.setTargetFileSizeBase(prop.getProperty("rocksdb.Options.setTargetFileSizeBase").toLong)
    } else {
      tableConfig.setBlockSize(prop.getProperty("rocksdb.BlockBasedTableConfig.setBlockSize").toLong)
      options.setWriteBufferSize(prop.getProperty("rocksdb.Options.setWriteBufferSize").toLong)
      options.setTargetFileSizeBase(prop.getProperty("rocksdb.Options.setTargetFileSizeBase").toLong)
    }

    options.setTableFormatConfig(tableConfig)
    options.setCreateIfMissing(createIfMissing)

    val compressionType = {
      prop.getProperty("rocksdb.Options.setCompressionType") match {
        case "CompressionType.LZ4_COMPRESSION" => CompressionType.LZ4_COMPRESSION
        case "CompressionType.BZLIB2_COMPRESSION" => CompressionType.BZLIB2_COMPRESSION
        case "CompressionType.DISABLE_COMPRESSION_OPTION" => CompressionType.DISABLE_COMPRESSION_OPTION
        case "CompressionType.LZ4HC_COMPRESSION" => CompressionType.LZ4HC_COMPRESSION
        case "CompressionType.NO_COMPRESSION" => CompressionType.NO_COMPRESSION
        case "CompressionType.SNAPPY_COMPRESSION" => CompressionType.SNAPPY_COMPRESSION
        case "CompressionType.XPRESS_COMPRESSION" => CompressionType.XPRESS_COMPRESSION
        case "CompressionType.ZLIB_COMPRESSION" => CompressionType.ZLIB_COMPRESSION
        case "CompressionType.ZSTD_COMPRESSION" => CompressionType.ZSTD_COMPRESSION
      }
    }
    options.setCompressionType(compressionType)
    options.setMaxOpenFiles(prop.getProperty("rocksdb.Options.setMaxOpenFiles").toInt)
    val compactionStyle = {
      prop.getProperty("rocksdb.Options.setCompactionStyle") match {
        case "CompactionStyle.LEVEL" => CompactionStyle.LEVEL
        case "CompactionStyle.FIFO" => CompactionStyle.FIFO
        case "CompactionStyle.NONE" => CompactionStyle.NONE
        case "CompactionStyle.UNIVERSAL" => CompactionStyle.UNIVERSAL
      }
    }
    options.setCompactionStyle(compactionStyle)
    options.setDisableAutoCompactions(prop.getProperty("rocksdb.Options.setDisableAutoCompactions").toBoolean)
    options.setLevel0FileNumCompactionTrigger(prop.getProperty("rocksdb.Options.setLevel0FileNumCompactionTrigger").toInt)
    options.setLevel0SlowdownWritesTrigger(prop.getProperty("rocksdb.Options.setLevel0SlowdownWritesTrigger").toInt)
    options.setLevel0StopWritesTrigger(prop.getProperty("rocksdb.Options.setLevel0StopWritesTrigger").toInt)
    options.setMaxBytesForLevelBase(prop.getProperty("rocksdb.Options.setMaxBytesForLevelBase").toLong)

    if (useForImporter) {
      options.setAllowConcurrentMemtableWrite(prop.getProperty("rocksdb.Options.importer.setAllowConcurrentMemtableWrite").toBoolean)
      options.setWriteBufferSize(prop.getProperty("rocksdb.Options.importer.setWriteBufferSize").toLong)
      options.setMaxWriteBufferNumber(prop.getProperty("rocksdb.Options.importer.setMaxWriteBufferNumber").toInt)
      options.setMinWriteBufferNumberToMerge(prop.getProperty("rocksdb.Options.importer.setMinWriteBufferNumberToMerge").toInt)
      options.setArenaBlockSize(prop.getProperty("rocksdb.Options.importer.setArenaBlockSize").toLong)
      options.setLevel0FileNumCompactionTrigger(prop.getProperty("rocksdb.Options.importer.setLevel0FileNumCompactionTrigger").toInt)
      options.setDisableAutoCompactions(prop.getProperty("rocksdb.Options.importer.setDisableAutoCompactions").toBoolean)
    }
    options
  }
}
