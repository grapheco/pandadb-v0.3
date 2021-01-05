package cn.pandadb.kernel.kv.index

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.queryparser.classic.{MultiFieldQueryParser, QueryParser}
import org.apache.lucene.search.{IndexSearcher, Query, ScoreDoc, TopDocs}
import org.apache.lucene.store.{Directory, FSDirectory}

import java.io.File

class FulltextIndexStore(val indexPath: String) {
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

  def insert(id: Long, props: Map[String, String]): Unit = {
    val document = new Document
    document.add(new StringField("_id", s"${id}", Store.YES))
    props.foreach { x =>
      document.add(new Field(x._1, x._2, termVectoredTextFieldType))
    }
    indexWriter.addDocument(document)
    indexWriter.commit()
  }

  def delete(id: Long): Unit =
    indexWriter.deleteDocuments(new Term("_id", s"${id}"))

  private def document2Map(doc: Document): collection.mutable.HashMap[String, String] = {
    val map = new collection.mutable.HashMap[String, String]
    val itl = doc.getFields().iterator()
    while (itl.hasNext()) {
      val field = itl.next()
      map += (field.name() -> field.stringValue())
    }
    map
  }

  private def scoreDoc2Map(doc: ScoreDoc): collection.mutable.HashMap[String, String] = {
    document2Map(reader.document(doc.doc))
  }

  private def scoreDoc2NodeProperties(doc: ScoreDoc): Map[String, String] = {
    val node = scoreDoc2Map(doc)
    node + ("_id" -> node.get("_id").get) toMap
  }

  def topDocs2NodeWithPropertiesArray(docs: TopDocs): Iterator[Map[String, String]] =
    docs.scoreDocs.map(scoreDoc => {scoreDoc2NodeProperties(scoreDoc)}).toIterator

  def topDocs2NodeIdArray(docs: TopDocs): Iterator[Long] =
    docs.scoreDocs.map(s=>{reader.document(s.doc).get("_id").toLong}).toIterator

  private def executeQuery(q: Query, topN: Int = Int.MaxValue): TopDocs = {
    val newReader = DirectoryReader.openIfChanged(reader)
    if (newReader != null) {
      reader.close()
      reader = newReader
      searcher = new IndexSearcher(reader)
    }
    searcher.search(q, topN)
  }

  def search(fields: Array[String], keyword: String, topN: Int = Int.MaxValue): TopDocs = {
    val stringQuery = new MultiFieldQueryParser(fields,analyzer).parse(keyword)
    executeQuery(stringQuery, topN)
  }

  def at(id: Int): Option[Map[String, String]] = {
    val idQuery = new QueryParser("_id", analyzer).parse(s"${id}")
    val hits = executeQuery(idQuery,1)
    if (hits.totalHits == 0) return None
    Some(scoreDoc2NodeProperties(hits.scoreDocs.head))
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
