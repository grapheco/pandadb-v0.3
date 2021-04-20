package cn.pandadb.tools.importer

import cn.pandadb.kernel.kv.db.KeyValueDB
import cn.pandadb.kernel.kv.meta.Statistics

import java.util.concurrent.atomic.AtomicLong

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 15:07 2021/1/20
 * @Modified By:
 */
case class GlobalArgs(coreNum: Int = Runtime.getRuntime().availableProcessors(),
                      globalNodeCount: AtomicLong, globalNodePropCount: AtomicLong,
                      globalRelCount: AtomicLong, globalRelPropCount: AtomicLong,
                      estNodeCount: Long, estRelCount: Long,
                      nodeDB: KeyValueDB, nodeLabelDB: KeyValueDB,
                      relationDB: KeyValueDB, inrelationDB: KeyValueDB, outRelationDB: KeyValueDB, relationTypeDB: KeyValueDB, statistics: Statistics)
