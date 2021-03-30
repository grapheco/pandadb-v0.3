package cn.pandadb.kernel.blob.impl

import java.util.Properties
import eu.medsea.mimeutil.MimeUtil
import org.apache.commons.io.IOUtils
import cn.pandadb.kernel.blob.api.{InputStreamSource, MimeType}

import scala.collection.JavaConversions._

/**
  * Created by bluejoe on 2019/4/18.
  */
object MimeTypeFactory {
  MimeUtil.registerMimeDetector("eu.medsea.mimeutil.detector.MagicMimeMimeDetector");

  val properties = new Properties();
  properties.load(this.getClass.getClassLoader.getResourceAsStream("mime.properties"));
  val code2Types = properties.map(x => (x._1.toLong, x._2.toLowerCase())).toMap;
  val type2Codes = code2Types.map(x => (x._2, x._1)).toMap;

  def fromText(text: String): MimeType = {
    val lc = text.toLowerCase();
    new MimeType(type2Codes.get(lc).getOrElse(throw new UnknownMimeTypeException(lc)), lc);
  }

  def fromCode(code: Long) = new MimeType(code, code2Types(code));

  def guessMimeType(iss: InputStreamSource): MimeType = {
    val mimeTypes = iss.offerStream { is =>
      MimeUtil.getMimeTypes(IOUtils.toByteArray(is))
    }.toList;

    mimeTypes.headOption.map(mt => fromText(mt.toString)).getOrElse(fromCode(-1));
  }
}

class UnknownMimeTypeException(mtype: String) extends RuntimeException(s"unknown mime-type: $mtype") {

}