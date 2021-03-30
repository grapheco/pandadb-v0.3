package cn.pandadb.kernel.blob.api.util

import org.slf4j.LoggerFactory

trait Logging {
  val logger = LoggerFactory.getLogger(this.getClass);
}
