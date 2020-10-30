package cn.pandadb.kernel

import java.io.{File, FileNotFoundException}

import cn.pandadb.kernel.impl.SimpleGraphRAM
import cn.pandadb.kernel.store.{FileBasedIdGen, LabelStore, LogStore, NodeStore, RelationStore}
import cn.pandadb.kernel.util.LockFile

import scala.collection.mutable

object PandaDB {
  def build(dir: File, configs: Map[String, String]): GraphFacade = {
    val root = dir.getAbsoluteFile.getCanonicalFile

    if (!dir.exists())
      throw new FileNotFoundException(root.getPath)

    val nodes = new NodeStore(new File(root, "nodes"))
    val rels = new RelationStore(new File(root, "rels"))
    val logs = new LogStore(new File(root, "logs"))
    val nodelabels = new LabelStore(new File(root, "nodelabels"))
    val rellabels = new LabelStore(new File(root, "rellabels"), Int.MaxValue)

    val lockFile = new LockFile(new File(root, ".lock"))
    lockFile.assertUnlocked()
    lockFile.lock()

    val facade = new GraphFacade(nodes, rels, logs, nodelabels, rellabels,
      new FileBasedIdGen(new File(root, "nodeid"), 100),
      new FileBasedIdGen(new File(root, "relid"), 100),
      new SimpleGraphRAM(),
      new PropertyStore {
        val propStore = mutable.Map[TypedId, mutable.Map[String, Any]]()

        override def insert(id: TypedId, props: Map[String, Any]): Unit =
          propStore += id -> (mutable.Map[String, Any]() ++ props)

        override def delete(id: TypedId): Unit = propStore -= id

        override def lookup(id: TypedId): Option[Map[String, Any]] = propStore.get(id).map(_.toMap)

        override def close(): Unit = {
        }
      }, {
        lockFile.unlock()
      })

    facade
  }
}
