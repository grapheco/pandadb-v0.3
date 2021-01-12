package cn.pandadb.driver

import java.lang
import java.util.concurrent.CompletionStage
import java.util.function.Consumer

import org.neo4j.driver.async.AsyncSession
import org.neo4j.driver.reactive.RxSession
import org.neo4j.driver.types.TypeSystem
import org.neo4j.driver.{Driver, Metrics, Session, SessionConfig}

class PandaDriver(rpcClient: PandaRpcClient) extends Driver{
  override def isEncrypted: Boolean = false

  override def session(): Session = new PandaSession(rpcClient)

  override def session(sessionConfig: SessionConfig): Session = new PandaSession(rpcClient)

  override def rxSession(): RxSession = throw new NotImplementMethodException("rxSession")

  override def rxSession(sessionConfig: SessionConfig): RxSession = throw new NotImplementMethodException("rxSession")

  override def asyncSession(): AsyncSession = throw new NotImplementMethodException("asyncSession")

  override def asyncSession(sessionConfig: SessionConfig): AsyncSession = throw new NotImplementMethodException("asyncSession")

  override def close(): Unit = {rpcClient.close}

  override def closeAsync(): CompletionStage[Void] = throw new NotImplementMethodException("closeAsync")

  override def metrics(): Metrics = throw new NotImplementMethodException("metrics")

  override def isMetricsEnabled: Boolean = false

  override def defaultTypeSystem(): TypeSystem = {
    throw new NotImplementMethodException("defaultTypeSystem")
  }

  override def verifyConnectivity(): Unit = {}

  override def verifyConnectivityAsync(): CompletionStage[Void] = throw new NotImplementMethodException("verifyConnectivityAsync")

  override def supportsMultiDb(): Boolean = false

  override def supportsMultiDbAsync(): CompletionStage[lang.Boolean] = throw new NotImplementMethodException("supportsMultiDbAsync")
}
