package cn.pandadb.driver

import java.lang
import java.util.concurrent.CompletionStage

import cn.pandadb.NotImplementMethodException
import cn.pandadb.hipporpc.PandaRpcClient
import org.neo4j.driver.v1.{AccessMode, Driver, Session}

class PandaDriver(rpcClient: PandaRpcClient, address: String) extends Driver{
  override def isEncrypted: Boolean = false

  override def session(): Session = new PandaSession(rpcClient, address)

  override def session(accessMode: AccessMode): Session = {
    new PandaSession(rpcClient, address)
  }

  override def session(s: String): Session = {
    println(s)
    new PandaSession(rpcClient, address)
  }

  override def session(accessMode: AccessMode, s: String): Session = {
    new PandaSession(rpcClient, address)
  }

  override def session(iterable: lang.Iterable[String]): Session = {
    new PandaSession(rpcClient, address)
  }

  override def session(accessMode: AccessMode, iterable: lang.Iterable[String]): Session = {
    new PandaSession(rpcClient, address)
  }

  override def close(): Unit = {rpcClient.close}

  override def closeAsync(): CompletionStage[Void] = throw new NotImplementMethodException("closeAsync")
}