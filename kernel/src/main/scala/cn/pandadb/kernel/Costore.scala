package cn.pandadb.kernel

import java.io.File

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.queryparser.classic.{MultiFieldQueryParser, QueryParser}
import org.apache.lucene.search.{IndexSearcher, Query, ScoreDoc, TopDocs}
import org.apache.lucene.store.{Directory, FSDirectory}

class Costore(val indexPath: String) {
  val dir = new File(indexPath)
  val directory: Directory = FSDirectory.open(dir.toPath)
  val analyzer = new StandardAnalyzer
  val conf = new IndexWriterConfig(analyzer)
  val indexWriter = new IndexWriter(directory, conf)
  var reader = DirectoryReader.open(indexWriter)
  var searcher = new IndexSearcher(reader)

  val termVectoredTextFieldType = new FieldType(TextField.TYPE_STORED)
  termVectoredTextFieldType.setStoreTermVectors(true)
  termVectoredTextFieldType.setStoreTermVectorOffsets(true)
  termVectoredTextFieldType.setStoreTermVectorPositions(true)

  private def documentId(tid: TypedId): String = {
    tid match {
      case NodeId(id) => s"1_$id"
      case RelationId(id) => s"2_$id"
    }
  }

  def insert(id: TypedId, props: Map[String, Any]): Unit = {
    val document = new Document
    document.add(new StringField("_id", documentId(id), Store.YES))
    props.foreach { x =>
      document.add(new Field(x._1, x._2.toString, termVectoredTextFieldType))
    }
    indexWriter.addDocument(document)
    indexWriter.commit()
  }

  def delete(id: TypedId): Unit =
    indexWriter.deleteDocuments(new Term("_id", documentId(id)))

  private def document2Map(doc: Document): collection.mutable.HashMap[String, Any] = {
    val map = new collection.mutable.HashMap[String, Any]
    val itl = doc.getFields().iterator()
    while (itl.hasNext()) {
      val field = itl.next()
      map += (field.name() -> field.stringValue())
    }
    map
  }

  private def scoreDoc2Map(doc: ScoreDoc): collection.mutable.HashMap[String, Any] = {
    document2Map(reader.document(doc.doc))
  }

  private def scoreDoc2NodeWithProperties(doc: ScoreDoc): Map[String, Any] = {
    val node = scoreDoc2Map(doc)
    node + ("id" -> node.get("_id").get.asInstanceOf[String].drop(2).toLong) - "_id" toMap
  }

  def topDocs2NodeWithPropertiesArray(docs: TopDocs): Option[Array[Map[String, Any]]] = {
    if (docs.totalHits == 0) return None
    val array = new collection.mutable.ArrayBuffer[Map[String, Any]]
    docs.scoreDocs.map(scoreDoc => {
      array += scoreDoc2NodeWithProperties(scoreDoc)
    })
    Some(array.toArray)
  }

  private def executeQuery(q: Query, topN: Int = Int.MaxValue): TopDocs = {
    val newReader = DirectoryReader.openIfChanged(reader)
    if (newReader != null) {
      reader.close()
      reader = newReader
      searcher = new IndexSearcher(reader)
    }
    searcher.search(q, topN)
  }

  def search(keyword: (Array[String], String), topN: Int = Int.MaxValue): TopDocs = {
    val stringQuery = new MultiFieldQueryParser(keyword._1,analyzer).parse(keyword._2)
    executeQuery(stringQuery, topN)
  }

  def lookup(id: TypedId): Option[Map[String, Any]] = {
    val idQuery = new QueryParser("_id", analyzer).parse(documentId(id))
    val hits = executeQuery(idQuery,1)
    if (hits.totalHits == 0) return None
    Some(scoreDoc2NodeWithProperties(hits.scoreDocs.head))
  }

  def dropAndClose(): Unit ={
    indexWriter.deleteAll()
    indexWriter.close()
    reader.close()
    directory.listAll().foreach(directory.deleteFile(_))
    dir.delete()
  }

  def close(): Unit = {
    indexWriter.close()
    reader.close()
    directory.close()
  }

}
