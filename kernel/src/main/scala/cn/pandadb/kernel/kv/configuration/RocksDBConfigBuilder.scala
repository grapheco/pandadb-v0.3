package cn.pandadb.kernel.kv.configuration

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import org.rocksdb.{AccessHint, BlockBasedTableConfig, BloomFilter, Cache, ChecksumType, CompactionStyle, CompressionType, IndexType, LRUCache, Options, ReadOptions, Statistics}

class RocksDBConfigBuilder(rocksdbConfigFilePath: String) extends LazyLogging{
  implicit def strToBoolean(str: String) = str.toBoolean

  def getOptions(): Options = {
    val file = new File(rocksdbConfigFilePath)

    if (!file.exists()) {
      return null
    }

    val prop = new Properties()
    val is = new BufferedInputStream(new FileInputStream(file))
    prop.load(is)
    logger.info(s"settings nums: ${prop.keySet().size()}")
    val options: Options = new Options()
    val tableConfig = new BlockBasedTableConfig()

    val keys = prop.keySet().asScala.map(x => x.toString)
    keys.foreach {
      case "options.setCompressionType" => {
        val compressiontype = {
          val str = prop.getProperty("options.setCompressionType")
          str match {
            case "BZLIB2_COMPRESSION" => CompressionType.BZLIB2_COMPRESSION
            case "DISABLE_COMPRESSION_OPTION" => CompressionType.DISABLE_COMPRESSION_OPTION
            case "LZ4_COMPRESSION" => CompressionType.LZ4_COMPRESSION
            case "LZ4HC_COMPRESSION" => CompressionType.LZ4HC_COMPRESSION
            case "NO_COMPRESSION" => CompressionType.NO_COMPRESSION
            case "SNAPPY_COMPRESSION" => CompressionType.SNAPPY_COMPRESSION
            case "XPRESS_COMPRESSION" => CompressionType.XPRESS_COMPRESSION
            case "ZLIB_COMPRESSION" => CompressionType.ZLIB_COMPRESSION
            case "ZSTD_COMPRESSION" => CompressionType.ZSTD_COMPRESSION
            case _ => throw new Exception("no such settings")
          }
        }
        options.setCompressionType(compressiontype)
      }

      case "options.setOptimizeFiltersForHits()" => {
        options.setOptimizeFiltersForHits(prop.getProperty("options.setOptimizeFiltersForHits()"))
      }

      case "options.setCreateIfMissing" => {
        options.setCreateIfMissing(prop.getProperty("options.setCreateIfMissing"))
      }

      case "options.setLevel0StopWritesTrigger" => {
        options.setLevel0StopWritesTrigger(prop.getProperty("options.setLevel0StopWritesTrigger").toInt)
      }

      case "options.setTargetFileSizeMultiplier" => {
        options.setTargetFileSizeMultiplier(prop.getProperty("options.setTargetFileSizeMultiplier").toInt)
      }
      case "options.setLevel0SlowdownWritesTrigger" => {
        options.setLevel0SlowdownWritesTrigger(prop.getProperty("options.setLevel0SlowdownWritesTrigger").toInt)
      }
      case "options.setDisableAutoCompactions" => {
        options.setDisableAutoCompactions(prop.getProperty("options.setDisableAutoCompactions"))
      }
      case "options.setMinWriteBufferNumberToMerge" => {
        options.setMinWriteBufferNumberToMerge(prop.getProperty("options.setMinWriteBufferNumberToMerge").toInt)
      }
      case "options.setMaxBytesForLevelBase" => {
        options.setMaxBytesForLevelBase(prop.getProperty("options.setMaxBytesForLevelBase").toLong)
      }
      case "options.setMaxWriteBufferNumber" => {
        options.setMaxWriteBufferNumber(prop.getProperty("options.setMaxWriteBufferNumber").toInt)
      }
      case "tableConfig.setBlockSize" => {
        tableConfig.setBlockSize(prop.getProperty("tableConfig.setBlockSize").toLong)
      }
      case "tableConfig.setBlockCache" => {
        val num = prop.getProperty("tableConfig.setBlockCache").toLong
        tableConfig.setBlockCache(new LRUCache(num))
      }
      case "options.setTargetFileSizeBase" => {
        options.setTargetFileSizeBase(prop.getProperty("options.setTargetFileSizeBase").toLong)
      }
      case "options.setSkipCheckingSstFileSizesOnDbOpen" => {
        options.setSkipCheckingSstFileSizesOnDbOpen(prop.getProperty("options.setSkipCheckingSstFileSizesOnDbOpen"))
      }
      case "tableConfig.setCacheIndexAndFilterBlocks" => {
        tableConfig.setCacheIndexAndFilterBlocks(prop.getProperty("tableConfig.setCacheIndexAndFilterBlocks"))
      }
      case "options.setMaxBackgroundJobs" => {
        options.setMaxBackgroundJobs(prop.getProperty("options.setMaxBackgroundJobs").toInt)
      }
      case "options.setWriteBufferSize" => {
        options.setWriteBufferSize(prop.getProperty("options.setWriteBufferSize").toLong)
      }
      case "options.setMaxOpenFiles" => {
        options.setMaxOpenFiles(prop.getProperty("options.setMaxOpenFiles").toInt)
      }
      case "options.setLevel0FileNumCompactionTrigger" => {
        options.setLevel0FileNumCompactionTrigger(prop.getProperty("options.setLevel0FileNumCompactionTrigger").toInt)
      }
      case "options.setAllowConcurrentMemtableWrite" => {
        options.setAllowConcurrentMemtableWrite(prop.getProperty("options.setAllowConcurrentMemtableWrite"))
      }
      case "options.setMaxBytesForLevelMultiplier" => {
        options.setMaxBytesForLevelMultiplier(prop.getProperty("options.setMaxBytesForLevelMultiplier").toDouble)
      }
      case "options.setLevelCompactionDynamicLevelBytes" => {
        options.setLevelCompactionDynamicLevelBytes(prop.getProperty("options.setLevelCompactionDynamicLevelBytes"))
      }
      case "options.setCompactionStyle" =>{
        val str = prop.getProperty("options.setCompactionStyle")
        str match {
          case "LEVEL" => options.setCompactionStyle(CompactionStyle.LEVEL)
          case "FIFO" => options.setCompactionStyle(CompactionStyle.FIFO)
          case "NONE" => options.setCompactionStyle(CompactionStyle.NONE)
          case "UNIVERSAL" => options.setCompactionStyle(CompactionStyle.UNIVERSAL)
          case _ => options.setCompactionStyle(CompactionStyle.LEVEL)
        }
      }
      case k => {
        throw new Exception(s"not support $k setting")
      }
    }
    tableConfig.setFilterPolicy(new BloomFilter(15, false))
    options.setTableFormatConfig(tableConfig)
    options
  }

}
