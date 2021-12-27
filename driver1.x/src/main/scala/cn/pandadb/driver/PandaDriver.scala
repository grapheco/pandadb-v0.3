package cn.pandadb.driver

import java.lang
import java.util.concurrent.CompletionStage

import cn.pandadb.NotImplementMethodException
import org.neo4j.driver.v1.{AccessMode, Driver, Session}

class PandaDriver(uriAuthority: String, config: PandaDriverConfig) extends Driver{
  override def isEncrypted: Boolean = false

  override def session(): Session = new PandaSession(uriAuthority, config)

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
  }

  override def closeAsync(): CompletionStage[Void] = throw new NotImplementMethodException("closeAsync")
}
