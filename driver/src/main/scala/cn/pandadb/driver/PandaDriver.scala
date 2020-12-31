package cn.pandadb.driver

import java.util.concurrent.CompletionStage
import java.util.function.Consumer

import org.neo4j.driver.async.AsyncSession
import org.neo4j.driver.reactive.RxSession
import org.neo4j.driver.{Driver, Metrics, Session, SessionParametersTemplate}

class PandaDriver(rpcClient: PandaRpcClient) extends Driver{

  override def isEncrypted: Boolean = false

  override def session(): Session = new PandaSession(rpcClient: PandaRpcClient)

  override def session(templateConsumer: Consumer[SessionParametersTemplate]): Session = throw new NotImplementMethodException

  override def close(): Unit = {
    rpcClient.close
  }

  override def closeAsync(): CompletionStage[Void] = throw new NotImplementMethodException

  override def metrics(): Metrics = throw new NotImplementMethodException

  override def rxSession(): RxSession = throw new NotImplementMethodException

  override def rxSession(templateConsumer: Consumer[SessionParametersTemplate]): RxSession = throw new NotImplementMethodException

  override def asyncSession(): AsyncSession = throw new NotImplementMethodException

  override def asyncSession(templateConsumer: Consumer[SessionParametersTemplate]): AsyncSession = throw new NotImplementMethodException
}
