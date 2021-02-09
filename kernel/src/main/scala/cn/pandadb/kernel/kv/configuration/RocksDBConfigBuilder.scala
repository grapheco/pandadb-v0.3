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
    keys.foreach(k => {
      k match {
        case "options.setCompressionType()" => {
          val compressiontype = {
            val str = prop.getProperty("options.setCompressionType()")
            str match {
              case "CompressionType.BZLIB2_COMPRESSION" => CompressionType.BZLIB2_COMPRESSION
              case "CompressionType.DISABLE_COMPRESSION_OPTION" => CompressionType.DISABLE_COMPRESSION_OPTION
              case "CompressionType.LZ4_COMPRESSION" => CompressionType.LZ4_COMPRESSION
              case "CompressionType.LZ4HC_COMPRESSION" => CompressionType.LZ4HC_COMPRESSION
              case "CompressionType.NO_COMPRESSION" => CompressionType.NO_COMPRESSION
              case "CompressionType.SNAPPY_COMPRESSION" => CompressionType.SNAPPY_COMPRESSION
              case "CompressionType.XPRESS_COMPRESSION" => CompressionType.XPRESS_COMPRESSION
              case "CompressionType.ZLIB_COMPRESSION" => CompressionType.ZLIB_COMPRESSION
              case "CompressionType.ZSTD_COMPRESSION" => CompressionType.ZSTD_COMPRESSION
              case _ => throw new Exception("no such settings")
            }
          }
          options.setCompressionType(compressiontype)
        }
        case "tableConfig.setChecksumType()" => {
          val str = prop.getProperty("tableConfig.setChecksumType()")
          val checksumType = {
            str match {
              case "ChecksumType.kCRC32c" => ChecksumType.kCRC32c
              case "ChecksumType.kNoChecksum" => ChecksumType.kNoChecksum
              case "ChecksumType.kxxHash" => ChecksumType.kxxHash
              case "ChecksumType.kxxHash64" => ChecksumType.kxxHash64
              case _ => throw new Exception("no such settings")
            }
          }
          tableConfig.setChecksumType(checksumType)
        }
        case "options.setTtl()" => {
          options.setTtl(prop.getProperty("options.setTtl()").toLong)
        }
        case "tableConfig.setPartitionFilters()" => {
          tableConfig.setPartitionFilters(prop.getProperty("tableConfig.setPartitionFilters()"))
        }
        case "options.setStrictBytesPerSync()" => {
          options.setStrictBytesPerSync(prop.getProperty("options.setStrictBytesPerSync()"))
        }
        case "options.setMaxTableFilesSizeFIFO()" => {
          options.setMaxTableFilesSizeFIFO(prop.getProperty("options.setMaxTableFilesSizeFIFO()").toLong)
        }
        case "options.setOptimizeFiltersForHits()" => {
          options.setOptimizeFiltersForHits(prop.getProperty("options.setOptimizeFiltersForHits()"))
        }
        case "options.setMaxFileOpeningThreads()" => {
          options.setMaxFileOpeningThreads(prop.getProperty("options.setMaxFileOpeningThreads()").toInt)
        }
        case "options.setMaxWriteBatchGroupSizeBytes()" => {
          options.setMaxWriteBatchGroupSizeBytes(prop.getProperty("options.setMaxWriteBatchGroupSizeBytes()").toLong)
        }
        case "options.setCreateIfMissing()" => {
          options.setCreateIfMissing(prop.getProperty("options.setCreateIfMissing()"))
        }
        case "options.setPersistStatsToDisk()" => {
          options.setPersistStatsToDisk(prop.getProperty("options.setPersistStatsToDisk()"))
        }
        case "options.setDumpMallocStats()" => {
          options.setDumpMallocStats(prop.getProperty("options.setDumpMallocStats()"))
        }
        case "options.setRandomAccessMaxBufferSize()" => {
          options.setRandomAccessMaxBufferSize(prop.getProperty("options.setRandomAccessMaxBufferSize()").toLong)
        }
        case "options.setLevel0StopWritesTrigger()" => {
          options.setLevel0StopWritesTrigger(prop.getProperty("options.setLevel0StopWritesTrigger()").toInt)
        }
        case "tableConfig.setOptimizeFiltersForMemory()" => {
          tableConfig.setOptimizeFiltersForMemory(prop.getProperty("tableConfig.setOptimizeFiltersForMemory()"))
        }
        case "options.setTargetFileSizeMultiplier()" => {
          options.setTargetFileSizeMultiplier(prop.getProperty("options.setTargetFileSizeMultiplier()").toInt)
        }
        case "options.setAvoidFlushDuringRecovery()" => {
          options.setAvoidFlushDuringRecovery(prop.getProperty("options.setAvoidFlushDuringRecovery()"))
        }
        case "options.setDeleteObsoleteFilesPeriodMicros()" => {
          options.setDeleteObsoleteFilesPeriodMicros(prop.getProperty("options.setDeleteObsoleteFilesPeriodMicros()").toLong)
        }
        case "options.setTwoWriteQueues()" => {
          options.setTwoWriteQueues(prop.getProperty("options.setTwoWriteQueues()"))
        }
        case "tableConfig.setVerifyCompression()" => {
          tableConfig.setVerifyCompression(prop.getProperty("tableConfig.setVerifyCompression()"))
        }
        case "options.setLevelZeroStopWritesTrigger()" => {
          options.setLevelZeroStopWritesTrigger(prop.getProperty("options.setLevelZeroStopWritesTrigger()").toInt)
        }
        case "options.setLevel0SlowdownWritesTrigger()" => {
          options.setLevel0SlowdownWritesTrigger(prop.getProperty("options.setLevel0SlowdownWritesTrigger()").toInt)
        }
        case "options.setAllowMmapReads()" => {
          options.setAllowMmapReads(prop.getProperty("options.setAllowMmapReads()"))
        }
        case "options.setManualWalFlush()" => {
          options.setManualWalFlush(prop.getProperty("options.setManualWalFlush()"))
        }
        case "options.setParanoidFileChecks()" => {
          options.setParanoidFileChecks(prop.getProperty("options.setParanoidFileChecks()"))
        }
        case "options.setLogReadaheadSize()" => {
          options.setLogReadaheadSize(prop.getProperty("options.setLogReadaheadSize()").toLong)
        }
        case "tableConfig.setNoBlockCache()" => {
          tableConfig.setNoBlockCache(prop.getProperty("tableConfig.setNoBlockCache()"))
        }
        case "options.setEnableThreadTracking()" => {
          options.setEnableThreadTracking(prop.getProperty("options.setEnableThreadTracking()"))
        }
        case "tableConfig.setDataBlockHashTableUtilRatio()" => {
          tableConfig.setDataBlockHashTableUtilRatio(prop.getProperty("tableConfig.setDataBlockHashTableUtilRatio()").toDouble)
        }
        case "tableConfig.setFormatVersion()" => {
          tableConfig.setFormatVersion(prop.getProperty("tableConfig.setFormatVersion()").toInt)
        }
        case "options.setMaxSuccessiveMerges()" => {
          options.setMaxSuccessiveMerges(prop.getProperty("options.setMaxSuccessiveMerges()").toLong)
        }
        case "options.setWalBytesPerSync()" => {
          options.setWalBytesPerSync(prop.getProperty("options.setWalBytesPerSync()").toLong)
        }
        case "options.setDisableAutoCompactions()" => {
          options.setDisableAutoCompactions(prop.getProperty("options.setDisableAutoCompactions()"))
        }
        case "options.setAdviseRandomOnOpen()" => {
          options.setAdviseRandomOnOpen(prop.getProperty("options.setAdviseRandomOnOpen()"))
        }
        case "options.setWriteThreadSlowYieldUsec()" => {
          options.setWriteThreadSlowYieldUsec(prop.getProperty("options.setWriteThreadSlowYieldUsec()").toLong)
        }
        case "options.setAtomicFlush()" => {
          options.setAtomicFlush(prop.getProperty("options.setAtomicFlush()"))
        }
        case "options.setMinWriteBufferNumberToMerge()" => {
          options.setMinWriteBufferNumberToMerge(prop.getProperty("options.setMinWriteBufferNumberToMerge()").toInt)
        }
        case "options.setStatsHistoryBufferSize()" => {
          options.setStatsHistoryBufferSize(prop.getProperty("options.setStatsHistoryBufferSize()").toLong)
        }
        case "options.setWritableFileMaxBufferSize()" => {
          options.setWritableFileMaxBufferSize(prop.getProperty("options.setWritableFileMaxBufferSize()").toLong)
        }
        case "options.setMaxBytesForLevelBase()" => {
          options.setMaxBytesForLevelBase(prop.getProperty("options.setMaxBytesForLevelBase()").toLong)
        }
        case "options.setAllowIngestBehind()" => {
          options.setAllowIngestBehind(prop.getProperty("options.setAllowIngestBehind()"))
        }
        case "options.setMemtableHugePageSize()" => {
          options.setMemtableHugePageSize(prop.getProperty("options.setMemtableHugePageSize()").toLong)
        }
        case "options.setMaxManifestFileSize()" => {
          options.setMaxManifestFileSize(prop.getProperty("options.setMaxManifestFileSize()").toLong)
        }
        case "tableConfig.setEnableIndexCompression()" => {
          tableConfig.setEnableIndexCompression(prop.getProperty("tableConfig.setEnableIndexCompression()"))
        }
        case "options.setWriteDbidToManifest()" => {
          options.setWriteDbidToManifest(prop.getProperty("options.setWriteDbidToManifest()"))
        }
        case "options.setMaxWriteBufferNumber()" => {
          options.setMaxWriteBufferNumber(prop.getProperty("options.setMaxWriteBufferNumber()").toInt)
        }
        case "tableConfig.setBlockSize()" => {
          tableConfig.setBlockSize(prop.getProperty("tableConfig.setBlockSize()").toLong)
        }
        case "tableConfig.setBlockCache()" => {
          val num = prop.getProperty("tableConfig.setBlockCache()").toLong
          tableConfig.setBlockCache(new LRUCache(num))
        }
        case "options.setMaxTotalWalSize()" => {
          options.setMaxTotalWalSize(prop.getProperty("options.setMaxTotalWalSize()").toLong)
        }
        case "options.setFailIfOptionsFileError()" => {
          options.setFailIfOptionsFileError(prop.getProperty("options.setFailIfOptionsFileError()"))
        }
        case "options.setMemtablePrefixBloomSizeRatio()" => {
          options.setMemtablePrefixBloomSizeRatio(prop.getProperty("options.setMemtablePrefixBloomSizeRatio()").toDouble)
        }
        case "tableConfig.setPinL0FilterAndIndexBlocksInCache()" => {
          tableConfig.setPinL0FilterAndIndexBlocksInCache(prop.getProperty("tableConfig.setPinL0FilterAndIndexBlocksInCache()"))
        }
        case "options.setMaxSequentialSkipInIterations()" => {
          options.setMaxSequentialSkipInIterations(prop.getProperty("options.setMaxSequentialSkipInIterations()").toLong)
        }
        case "options.setEnablePipelinedWrite()" => {
          options.setEnablePipelinedWrite(prop.getProperty("options.setEnablePipelinedWrite()"))
        }
        case "options.setTargetFileSizeBase()" => {
          options.setTargetFileSizeBase(prop.getProperty("options.setTargetFileSizeBase()").toLong)
        }
        case "options.setDelayedWriteRate()" => {
          options.setDelayedWriteRate(prop.getProperty("options.setDelayedWriteRate()").toLong)
        }
        case "options.setStatsDumpPeriodSec()" => {
          options.setStatsDumpPeriodSec(prop.getProperty("options.setStatsDumpPeriodSec()").toInt)
        }
        case "options.setUnorderedWrite()" => {
          options.setUnorderedWrite(prop.getProperty("options.setUnorderedWrite()"))
        }
        case "options.setArenaBlockSize()" => {
          options.setArenaBlockSize(prop.getProperty("options.setArenaBlockSize()").toLong)
        }
        case "options.setUseDirectReads()" => {
          options.setUseDirectReads(prop.getProperty("options.setUseDirectReads()"))
        }
        case "options.setEnableWriteThreadAdaptiveYield()" => {
          options.setEnableWriteThreadAdaptiveYield(prop.getProperty("options.setEnableWriteThreadAdaptiveYield()"))
        }
        case "tableConfig.setWholeKeyFiltering()" => {
          tableConfig.setWholeKeyFiltering(prop.getProperty("tableConfig.setWholeKeyFiltering()"))
        }
        case "tableConfig.setPinTopLevelIndexAndFilter()" => {
          tableConfig.setPinTopLevelIndexAndFilter(prop.getProperty("tableConfig.setPinTopLevelIndexAndFilter()"))
        }
        case "options.setMaxLogFileSize()" => {
          options.setMaxLogFileSize(prop.getProperty("options.setMaxLogFileSize()").toLong)
        }
        case "tableConfig.setMetadataBlockSize()" => {
          tableConfig.setMetadataBlockSize(prop.getProperty("tableConfig.setMetadataBlockSize()").toLong)
        }
        case "options.setIncreaseParallelism()" => {
          options.setIncreaseParallelism(prop.getProperty("options.setIncreaseParallelism()").toInt)
        }
        case "options.setReportBgIoStats()" => {
          options.setReportBgIoStats(prop.getProperty("options.setReportBgIoStats()"))
        }
        case "options.setAllowFAllocate()" => {
          options.setAllowFAllocate(prop.getProperty("options.setAllowFAllocate()"))
        }
        case "options.setCompactionReadaheadSize()" => {
          options.setCompactionReadaheadSize(prop.getProperty("options.setCompactionReadaheadSize()").toLong)
        }
        case "options.setKeepLogFileNum()" => {
          options.setKeepLogFileNum(prop.getProperty("options.setKeepLogFileNum()").toLong)
        }
        case "options.setWriteThreadMaxYieldUsec()" => {
          options.setWriteThreadMaxYieldUsec(prop.getProperty("options.setWriteThreadMaxYieldUsec()").toLong)
        }
        case "options.setParanoidChecks()" => {
          options.setParanoidChecks(prop.getProperty("options.setParanoidChecks()"))
        }
        case "options.setTableCacheNumshardbits()" => {
          options.setTableCacheNumshardbits(prop.getProperty("options.setTableCacheNumshardbits()").toInt)
        }
        case "tableConfig.setIndexBlockRestartInterval()" => {
          tableConfig.setIndexBlockRestartInterval(prop.getProperty("tableConfig.setIndexBlockRestartInterval()").toInt)
        }
        case "options.setForceConsistencyChecks()" => {
          options.setForceConsistencyChecks(prop.getProperty("options.setForceConsistencyChecks()"))
        }
        case "options.setLogFileTimeToRoll()" => {
          options.setLogFileTimeToRoll(prop.getProperty("options.setLogFileTimeToRoll()").toLong)
        }
        case "options.setSkipCheckingSstFileSizesOnDbOpen()" => {
          options.setSkipCheckingSstFileSizesOnDbOpen(prop.getProperty("options.setSkipCheckingSstFileSizesOnDbOpen()"))
        }
        case "options.setLevelZeroFileNumCompactionTrigger()" => {
          options.setLevelZeroFileNumCompactionTrigger(prop.getProperty("options.setLevelZeroFileNumCompactionTrigger()").toInt)
        }
        case "options.setAvoidUnnecessaryBlockingIO()" => {
          options.setAvoidUnnecessaryBlockingIO(prop.getProperty("options.setAvoidUnnecessaryBlockingIO()"))
        }
        case "tableConfig.setReadAmpBytesPerBit()" => {
          tableConfig.setReadAmpBytesPerBit(prop.getProperty("tableConfig.setReadAmpBytesPerBit()").toInt)
        }
        case "options.setDbWriteBufferSize()" => {
          options.setDbWriteBufferSize(prop.getProperty("options.setDbWriteBufferSize()").toLong)
        }
        case "options.setUseFsync()" => {
          options.setUseFsync(prop.getProperty("options.setUseFsync()"))
        }
        case "tableConfig.setCacheIndexAndFilterBlocksWithHighPriority()" => {
          tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(prop.getProperty("tableConfig.setCacheIndexAndFilterBlocksWithHighPriority()"))
        }
        case "options.setNumLevels()" => {
          options.setNumLevels(prop.getProperty("options.setNumLevels()").toInt)
        }
        case "tableConfig.setUseDeltaEncoding()" => {
          tableConfig.setUseDeltaEncoding(prop.getProperty("tableConfig.setUseDeltaEncoding()"))
        }
        case "options.setInplaceUpdateNumLocks()" => {
          options.setInplaceUpdateNumLocks(prop.getProperty("options.setInplaceUpdateNumLocks()").toLong)
        }
        case "options.setMaxCompactionBytes()" => {
          options.setMaxCompactionBytes(prop.getProperty("options.setMaxCompactionBytes()").toLong)
        }
        case "options.setInplaceUpdateSupport()" => {
          options.setInplaceUpdateSupport(prop.getProperty("options.setInplaceUpdateSupport()"))
        }
        case "options.setErrorIfExists()" => {
          options.setErrorIfExists(prop.getProperty("options.setErrorIfExists()"))
        }
        case "options.setIsFdCloseOnExec()" => {
          options.setIsFdCloseOnExec(prop.getProperty("options.setIsFdCloseOnExec()"))
        }
        case "tableConfig.setBlockAlign()" => {
          tableConfig.setBlockAlign(prop.getProperty("tableConfig.setBlockAlign()"))
        }
        case "tableConfig.setBlockSizeDeviation()" => {
          tableConfig.setBlockSizeDeviation(prop.getProperty("tableConfig.setBlockSizeDeviation()").toInt)
        }
        case "options.setUseDirectIoForFlushAndCompaction()" => {
          options.setUseDirectIoForFlushAndCompaction(prop.getProperty("options.setUseDirectIoForFlushAndCompaction()"))
        }
        case "options.setStatsPersistPeriodSec()" => {
          options.setStatsPersistPeriodSec(prop.getProperty("options.setStatsPersistPeriodSec()").toInt)
        }
        case "options.setNewTableReaderForCompactionInputs()" => {
          options.setNewTableReaderForCompactionInputs(prop.getProperty("options.setNewTableReaderForCompactionInputs()"))
        }
        case "options.setSoftPendingCompactionBytesLimit()" => {
          options.setSoftPendingCompactionBytesLimit(prop.getProperty("options.setSoftPendingCompactionBytesLimit()").toLong)
        }
        case "options.setAllowMmapWrites()" => {
          options.setAllowMmapWrites(prop.getProperty("options.setAllowMmapWrites()"))
        }
        case "options.setMaxBgErrorResumeCount()" => {
          options.setMaxBgErrorResumeCount(prop.getProperty("options.setMaxBgErrorResumeCount()").toInt)
        }
        case "tableConfig.setCacheIndexAndFilterBlocks()" => {
          tableConfig.setCacheIndexAndFilterBlocks(prop.getProperty("tableConfig.setCacheIndexAndFilterBlocks()"))
        }
        case "options.setMaxBackgroundJobs()" => {
          options.setMaxBackgroundJobs(prop.getProperty("options.setMaxBackgroundJobs()").toInt)
        }
        case "options.setHardPendingCompactionBytesLimit()" => {
          options.setHardPendingCompactionBytesLimit(prop.getProperty("options.setHardPendingCompactionBytesLimit()").toLong)
        }
        case "options.setWriteBufferSize()" => {
          options.setWriteBufferSize(prop.getProperty("options.setWriteBufferSize()").toLong)
        }
        case "options.setMaxOpenFiles()" => {
          options.setMaxOpenFiles(prop.getProperty("options.setMaxOpenFiles()").toInt)
        }
        case "options.setMaxSubcompactions()" => {
          options.setMaxSubcompactions(prop.getProperty("options.setMaxSubcompactions()").toInt)
        }
        case "options.setWalTtlSeconds()" => {
          options.setWalTtlSeconds(prop.getProperty("options.setWalTtlSeconds()").toLong)
        }
        case "options.setWalSizeLimitMB()" => {
          options.setWalSizeLimitMB(prop.getProperty("options.setWalSizeLimitMB()").toLong)
        }
        case "options.setUseAdaptiveMutex()" => {
          options.setUseAdaptiveMutex(prop.getProperty("options.setUseAdaptiveMutex()"))
        }
        case "options.setAllow2pc()" => {
          options.setAllow2pc(prop.getProperty("options.setAllow2pc()"))
        }
        case "options.setMaxWriteBufferNumberToMaintain()" => {
          options.setMaxWriteBufferNumberToMaintain(prop.getProperty("options.setMaxWriteBufferNumberToMaintain()").toInt)
        }
        case "options.setLevel0FileNumCompactionTrigger()" => {
          options.setLevel0FileNumCompactionTrigger(prop.getProperty("options.setLevel0FileNumCompactionTrigger()").toInt)
        }
        case "options.setPreserveDeletes()" => {
          options.setPreserveDeletes(prop.getProperty("options.setPreserveDeletes()"))
        }
        case "options.setRecycleLogFileNum()" => {
          options.setRecycleLogFileNum(prop.getProperty("options.setRecycleLogFileNum()").toLong)
        }
        case "tableConfig.setBlockRestartInterval()" => {
          tableConfig.setBlockRestartInterval(prop.getProperty("tableConfig.setBlockRestartInterval()").toInt)
        }
        case "options.setManifestPreallocationSize()" => {
          options.setManifestPreallocationSize(prop.getProperty("options.setManifestPreallocationSize()").toLong)
        }
        case "options.setLevelZeroSlowdownWritesTrigger()" => {
          options.setLevelZeroSlowdownWritesTrigger(prop.getProperty("options.setLevelZeroSlowdownWritesTrigger()").toInt)
        }
        case "options.setAllowConcurrentMemtableWrite()" => {
          options.setAllowConcurrentMemtableWrite(prop.getProperty("options.setAllowConcurrentMemtableWrite()"))
        }
        case "options.setMaxBytesForLevelMultiplier()" => {
          options.setMaxBytesForLevelMultiplier(prop.getProperty("options.setMaxBytesForLevelMultiplier()").toDouble)
        }
        case "options.setSkipStatsUpdateOnDbOpen()" => {
          options.setSkipStatsUpdateOnDbOpen(prop.getProperty("options.setSkipStatsUpdateOnDbOpen()"))
        }
        case "options.setLevelCompactionDynamicLevelBytes()" => {
          options.setLevelCompactionDynamicLevelBytes(prop.getProperty("options.setLevelCompactionDynamicLevelBytes()"))
        }
        case _ => {
          println(k)
          throw new Exception("not support this kind of settings")
        }
      }
    })
    tableConfig.setFilterPolicy(new BloomFilter(15, false))
    options.setTableFormatConfig(tableConfig)
    options.setCompactionStyle(CompactionStyle.LEVEL)
    options
  }

}
