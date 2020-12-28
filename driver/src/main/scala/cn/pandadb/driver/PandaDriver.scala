//package cn.pandadb.driver
//
//import java.util.concurrent.CompletionStage
//import java.util.function.Consumer
//
//import org.neo4j.driver.async.AsyncSession
//import org.neo4j.driver.reactive.RxSession
//import org.neo4j.driver.{Driver, Metrics, Session, SessionParametersTemplate}
//
//class PandaDriver extends Driver{
//
//  override def isEncrypted: Boolean = ???
//
//  override def session(): Session = new PandaSession
//
//  override def session(templateConsumer: Consumer[SessionParametersTemplate]): Session = ???
//
//  override def close(): Unit = ???
//
//  override def closeAsync(): CompletionStage[Void] = ???
//
//  override def metrics(): Metrics = ???
//
//  override def rxSession(): RxSession = ???
//
//  override def rxSession(templateConsumer: Consumer[SessionParametersTemplate]): RxSession = ???
//
//  override def asyncSession(): AsyncSession = ???
//
//  override def asyncSession(templateConsumer: Consumer[SessionParametersTemplate]): AsyncSession = ???
//}
