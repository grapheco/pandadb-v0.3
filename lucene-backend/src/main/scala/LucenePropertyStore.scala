package cn.pandadb.kernel.lucene

import java.io.File

import cn.pandadb.kernel.{NodeId, PropertyStore, RelationId, TypedId}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, StoredField, StringField, TextField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, IndexableField, Term}
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.{Directory, FSDirectory}
import org.apache.lucene.queryparser.classic.QueryParser

class LucenePropertyStore(dir: File) extends PropertyStore {

  val directory: Directory = FSDirectory.open(dir.toPath)
  val analyzer = new StandardAnalyzer
  val conf = new IndexWriterConfig(analyzer)
  val indexWriter = new IndexWriter(directory, conf)

  val reader = DirectoryReader.open(indexWriter)
  var searcher = new IndexSearcher(reader)

  private def documentId(tid: TypedId): String = {
    tid match {
      case NodeId(id) => s"1_$id"
      case RelationId(id) => s"2_$id"
    }
  }

  override def insert(id: TypedId, props: Map[String, Any]): Unit = {
    val document = new Document
    document.add(new StoredField("_id", documentId(id)))
    props.foreach { x =>
      document.add(new StoredField(x._1, x._2.toString))
    }
    indexWriter.addDocument(document)
    indexWriter.commit()
  }

  override def delete(id: TypedId): Unit =
    indexWriter.deleteDocuments(new Term("_id", documentId(id)))

  override def lookup(id: TypedId): Option[Map[String, Any]] = {
    val doc = new collection.mutable.HashMap[String, Any]()
    val idQuery = new QueryParser("_id", analyzer).parse(documentId(id))
    val hits = searcher.search(idQuery,1)
    if (hits.totalHits == 0) return None
    val itl = searcher.doc(hits.scoreDocs.head.doc).getFields().iterator()
    while (itl.hasNext()) {
      val field = itl.next()
      doc.put(field.name(), field.stringValue())
    }
    Some(doc.toMap)
  }

  override def close(): Unit = {
    indexWriter.close()
    reader.close()
  }

}