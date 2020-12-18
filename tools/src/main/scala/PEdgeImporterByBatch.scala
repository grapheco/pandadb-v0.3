
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler, RelationStore, RocksDBGraphAPI, RocksDBStorage}
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Test {
  /*
  100w: 17s
  2yi:  3400s = 0.95h
   */
  def main(args: Array[String]): Unit = {
    // 100w edges: avg 17s
    val dbPath = "D:\\data\\rocksdbBatchTest"
    val edgeFile = new File("D:\\data\\edge_output.csv")
    val edgeHeadFile = new File("D:\\data\\edgeHeadFile.csv")
    val importer = new PEdgeImporterByBatch(dbPath, edgeFile, edgeHeadFile)
    importer.importEdges()

  }
}

class PEdgeImporterByBatch(dbPath: String, edgeFile: File, edgeHeadFile: File) {
  //  val dbPath = "D:\\data\\rocksdbBatch"
  private val inEdgeDB = RocksDBStorage.getDB(s"${dbPath}/inEdge")
  private val outEdgeDB = RocksDBStorage.getDB(s"${dbPath}/outEdge")
  private val relDB = RocksDBStorage.getDB(s"${dbPath}/rels")

  var propSortArr: Array[String] = null
  val headMap: Map[String, String] = _setEdgeHead()

  val arrayTest:ArrayBuffer[String] = new ArrayBuffer[String]()
  var countEdge = 0
  def importEdges(): Unit = {

    val writeOptions: WriteOptions = new WriteOptions()
    writeOptions.setDisableWAL(false)
    writeOptions.setIgnoreMissingColumnFamilies(true)
    writeOptions.setSync(false)

    val batchRelation: WriteBatch = new WriteBatch()
    val batchInEdge: WriteBatch = new WriteBatch()
    val batchOutEdge: WriteBatch = new WriteBatch()

    //    val iter = Source.fromFile(new File(("D:\\data\\edge_output.csv"))).getLines()
    val iter = Source.fromFile(edgeFile).getLines()

    var i = 0

    var sstime = System.currentTimeMillis()
    while (iter.hasNext) {
      if (i % 20000000 == 0) {
        val time1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
        println(s"${i / 20000000}% edges imported. $time1")
      }
      i += 1

      val tempEdge = _wrapEdge(iter.next().replace("\n", "").split(","))

      val keyBytesRelation = KeyHandler.relationKeyToBytes(tempEdge.relId)
      val map = Map("from" -> tempEdge.fromId, "to" -> tempEdge.toId, "labelId" -> tempEdge.edgeType,
        "category" -> 0, "property" -> tempEdge.propMap)
      val valueBytes = ByteUtils.mapToBytes(map)
      batchRelation.put(keyBytesRelation, valueBytes)

      val keyBytesInEdge = KeyHandler.inEdgeKeyToBytes(tempEdge.toId, tempEdge.edgeType, 0, tempEdge.fromId)
      val inEdgeValue = ByteUtils.mapToBytes(Map[String, Any]("id" -> tempEdge.relId, "property" -> tempEdge.propMap))
      batchInEdge.put(keyBytesInEdge, inEdgeValue)

      val keyBytesOutEdge = KeyHandler.outEdgeKeyToBytes(tempEdge.fromId, tempEdge.edgeType, 0, tempEdge.toId)
      val outEdgeValue = ByteUtils.mapToBytes(Map[String, Any]("id" -> tempEdge.relId, "property" -> tempEdge.propMap))
      batchOutEdge.put(keyBytesOutEdge, outEdgeValue)



      if (tempEdge.fromId == 3543605){
        arrayTest += tempEdge.fromId.toString + ":" + tempEdge.edgeType.toString + ":"+ 0.toString + ":" + tempEdge.toId.toString
      }

      if (i % 1000000 == 0) {
        relDB.write(writeOptions, batchRelation)
        inEdgeDB.write(writeOptions, batchInEdge)
        outEdgeDB.write(writeOptions, batchOutEdge)

        batchRelation.clear()
        batchInEdge.clear()
        batchOutEdge.clear()

        println(s"coming~~~100w :${System.currentTimeMillis() - sstime} ms")
        sstime = System.currentTimeMillis()
      }
    }
    println("zzzzz", arrayTest.size, arrayTest.toSet.size)
  }

  private def _setEdgeHead(): Map[String, String] = {
    var hMap: Map[String, String] = Map[String, String]()
    val headArr = Source.fromFile(edgeHeadFile).getLines().next().replace("\n", "").split(",")
    propSortArr = new Array[String](headArr.length - 4)
    // headArr[]: fromId, toId, edgetype, propName1:type, ...
    for (i <- 4 to headArr.length - 1) {
      val fieldArr = headArr(i).split(":")
      val propName: String = fieldArr(0)
      propSortArr(i - 4) = propName
      val propType: String = {
        if (fieldArr.length == 1) "string"
        else fieldArr(1).toLowerCase()
      }
      hMap += (propName -> propType)
    }
    hMap
  }

  private def _wrapEdge(lineArr: Array[String]): TempEdge = {
    val relId: Long = lineArr(0).toLong
    val fromId: Long = lineArr(1).toLong
    val toId: Long = lineArr(2).toLong
    val edgeType: Int = PDBMetaData.getTypeId(lineArr(3))

    var propMap: Map[String, Any] = Map[String, Any]()
    for (i <- 4 to lineArr.length - 1) {
      val propName = propSortArr(i - 4)
      val propValue: Any = {
        headMap(propName) match {
          case "long" => lineArr(i).toLong
          case "int" => lineArr(i).toInt
          case "boolean" => lineArr(i).toBoolean
          case "double" => lineArr(i).toDouble
          case _ => lineArr(i).replace("\"", "")
        }
      }
      propMap += (propName -> propValue)
    }
    TempEdge(relId, fromId, toId, edgeType, propMap)
  }
}


case class TempEdge(relId: Long, fromId: Long, toId: Long, edgeType: Int, propMap: Map[String, Any])

