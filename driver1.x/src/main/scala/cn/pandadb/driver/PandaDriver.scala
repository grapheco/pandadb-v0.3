package cn.pandadb.driver

import java.lang
import java.util.concurrent.CompletionStage

import cn.pandadb.NotImplementMethodException
import org.neo4j.driver.v1.{AccessMode, Driver, Session}

import scala.collection.mutable.ArrayBuffer

class PandaDriver(uriAuthority: String, config: PandaDriverConfig) extends Driver{
  val sessions = ArrayBuffer[PandaSession]()

  override def isEncrypted: Boolean = false

  override def session(): Session = {
    this.synchronized{
      val s = new PandaSession(uriAuthority, config)
      sessions += s
      s
    }
  }

  override def session(accessMode: AccessMode): Session = {
    new PandaSession(uriAuthority, config)
  }

  override def session(s: String): Session = {
    new PandaSession(uriAuthority, config)
  }

  override def session(accessMode: AccessMode, s: String): Session = {
    new PandaSession(uriAuthority, config)
  }

  override def session(iterable: lang.Iterable[String]): Session = {
    new PandaSession(uriAuthority, config)
  }

  override def session(accessMode: AccessMode, iterable: lang.Iterable[String]): Session = {
    new PandaSession(uriAuthority, config)
  }

  override def close(): Unit = {
    sessions.foreach(s => s.close())
  }

  override def closeAsync(): CompletionStage[Void] = throw new NotImplementMethodException("closeAsync")
}
