package cn.pandadb.kv.distributed

import java.nio.ByteBuffer

import cn.pandadb.kernel.distribute.{DistributedGraphFacade, PandaDistributeKVAPI}
import org.junit.Test
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.shade.com.google.protobuf.ByteString

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-26 10:20
 */
class ImporterDemoTest {
  val tikv ={
    val conf = TiConfiguration.createRawDefault("10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379")
    val session = TiSession.create(conf)
    session.createRawClient()
  }
  val db = new PandaDistributeKVAPI(tikv)


  @Test
  def deleteAllDataInDB(): Unit ={
    val left = ByteString.copyFrom(ByteBuffer.wrap(Array((0).toByte)))
    val right = ByteString.copyFrom(ByteBuffer.wrap(Array((-1).toByte)))
    tikv.deleteRange(left, right)
  }

  @Test
  def batchPut(): Unit ={
//    val i = db.scanPrefix(Array(1.toByte))
//    while (i.hasNext){
//      println(i.next().getKey.toString)
//    }
    val db = new DistributedGraphFacade()
    println(db.getNode(1))
  }
  @Test
  def put(): Unit ={
//    val f1: Future[Unit] = Future{nodeBatch.grouped(2000).foreach(batch => db.batchPut(batch))}
//    val f2: Future[Unit] = Future{labelBatch.grouped(2000).foreach(batch => db.batchPut(batch))}
//
//    Await.result(f1, Duration.Inf)
//    Await.result(f2, Duration.Inf)
  }
}
