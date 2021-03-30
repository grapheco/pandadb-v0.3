package cn.pandadb.kernel.blob.impl

import java.io._
import java.net.URL
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpGet, HttpHead, HttpOptions}
import org.apache.http.impl.client.HttpClientBuilder
import cn.pandadb.kernel.blob.api._

import scala.collection.mutable
/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created in 9:18 2021/3/30
 * @Modified By:
 */
object BlobFactory {

  var memBlobCache: mutable.HashMap[String, Blob] = new mutable.HashMap[String, Blob]()
  val httpClient = HttpClientBuilder.create().build();
  val EMPTY: Blob = fromBytes(Array[Byte]());

  private class BlobImpl(val streamSource: InputStreamSource, val length: Long, val mimeType: MimeType)
    extends Blob {
    def withId(id: BlobId): ManagedBlob = new ManagedBlobImpl(streamSource, length, mimeType, id);
  }

  private class BlobEntryImpl(val id: BlobId, val length: Long, val mimeType: MimeType)
    extends BlobEntry {
  }

  private class ManagedBlobImpl(val streamSource: InputStreamSource, override val length: Long, override val mimeType: MimeType, override val id: BlobId)
    extends BlobEntryImpl(id, length, mimeType) with ManagedBlob {
  }

  def makeStoredBlob(entry: BlobEntry, streamSource: InputStreamSource): ManagedBlob =
    new ManagedBlobImpl(streamSource, entry.length, entry.mimeType, entry.id)

  def withId(blob: Blob, id: BlobId): ManagedBlob =
    new ManagedBlobImpl(blob.streamSource, blob.length, blob.mimeType, id)

  def makeBlob(length: Long, mimeType: MimeType, streamSource: InputStreamSource): Blob =
    new BlobImpl(streamSource, length, mimeType);

  def makeEntry(id: BlobId, length: Long, mimeType: MimeType): BlobEntry =
    new BlobEntryImpl(id, length, mimeType);

  def makeEntry(id: BlobId, blob: Blob): BlobEntry =
    new BlobEntryImpl(id, blob.length, blob.mimeType);

  def fromBytes(bytes: Array[Byte]): Blob = {
    fromInputStreamSource(new InputStreamSource() {
      override def offerStream[T](consume: (InputStream) => T): T = {
        val fis = new ByteArrayInputStream(bytes);
        val t = consume(fis);
        fis.close();
        t;
      }
    }, bytes.length, Some(MimeTypeFactory.fromText("application/octet-stream")));
  }

  def fromInputStreamSource(iss: InputStreamSource, length: Long, mimeType: Option[MimeType] = None): Blob = {
    new BlobImpl(iss,
      length,
      mimeType.getOrElse(MimeTypeFactory.guessMimeType(iss)));
  }

  def fronURLInputStreamSource(uiss: URLInputStreamSource, length: Long, mimeType: Option[MimeType] = None): Blob = {
    new BlobImpl(uiss,
      length,
      mimeType.getOrElse(MimeTypeFactory.guessMimeType(uiss)));
  }

  def fromFile(file: File, mimeType: Option[MimeType] = None): Blob = {

    fromInputStreamSource(new InputStreamSource() {
      override def offerStream[T](consume: (InputStream) => T): T = {
        val fis = new FileInputStream(file);
        val t = consume(fis);
        fis.close();
        t;
      }
    },
      file.length(),
      mimeType);
  }

  def fromHttpURL(httpUrl: String): Blob = {
    val head = new HttpHead(httpUrl)
    val resp = httpClient.execute(head)
    val (len, mime) = resp.getFirstHeader("Content-Length").getValue.toInt -> resp.getFirstHeader("Content-Type").getValue
    resp.close()

    BlobFactory.fronURLInputStreamSource(new URLInputStreamSource {
      override val url: String = httpUrl

      override def offerStream[T](consume: InputStream => T): T = {
        val get = new HttpGet(url)
        val resp = httpClient.execute(get)
        val t = consume(resp.getEntity.getContent)
        resp.close()
        t
      }
    }, len, Some(MimeTypeFactory.fromText(mime)))
  }

  def fromURL(url: String): Blob = {
    val p = "(?i)(http|https|file|ftp|ftps):\\/\\/[\\w\\-_]+(\\.[\\w\\-_]+)+([\\w\\-\\.,@?^=%&:/~\\+#]*[\\w\\-\\@?^=%&/~\\+#])?".r
    val uri = p.findFirstIn(url).getOrElse(url)

    val lower = uri.toLowerCase();
    if (lower.startsWith("http://") || lower.startsWith("https://")) {
      fromHttpURL(uri);
    }
    else if (lower.startsWith("file://")) {
      fromFile(new File(uri.substring(lower.indexOf("//") + 1)));
    }
    else {
      //ftp, ftps?
      fromBytes(IOUtils.toByteArray(new URL(uri)));
    }
  }
}
