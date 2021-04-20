package cn.pandadb.blob

import org.apache.commons.io.IOUtils
import org.junit.{Assert, Test}
import cn.pandadb.kernel.blob.impl.BlobFactory
import cn.pandadb.kernel.blob.api.{Blob, URLInputStreamSource}

import java.io.{File, FileInputStream}
import java.net.URL

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created in 9:42 2021/3/30
 * @Modified By:
 */
class BlobFactoryTest {
  @Test
  def testHttpsBlob(): Unit = {
    val surl = "https://www.baidu.com/img/flexible/logo/pc/result.png"
    val blob = BlobFactory.fromHttpURL(surl)
    val url = new URL(surl)
    _testBlob(blob, 6617, "image/png", IOUtils.toByteArray(url))
//    Assert.assertEquals(surl, blob.streamSource.asInstanceOf[URLInputStreamSource].url)
  }

  @Test
  def testFileBlob(): Unit = {
    val file = new File("./testinput/result.png")
    val blob = BlobFactory.fromFile(file)
    _testBlob(blob, file.length(), "image/png", IOUtils.toByteArray(new FileInputStream(file)))
    val filePath = file.getCanonicalPath
//    Assert.assertEquals(filePath, blob.streamSource.asInstanceOf[URLInputStreamSource].url)
  }

  private def _testBlob(blob: Blob, expectedLength: Long, expectedMimeType: String, content: Array[Byte]): Unit = {
    Assert.assertEquals(expectedLength, blob.length)
    Assert.assertEquals(expectedMimeType, blob.mimeType.text)

    blob.offerStream { is =>
      Assert.assertArrayEquals(content, IOUtils.toByteArray(is))
    }

    //test repeatable
    blob.offerStream { is =>
      Assert.assertArrayEquals(content, IOUtils.toByteArray(is))
    }
  }

}
