package cn.pandadb.kernel.blob.api.util

import java.io.File

/**
  * Created by bluejoe on 2019/7/23.
  */
trait Configuration {
  def getRaw(name: String): Option[String];
}

/**
  * Created by bluejoe on 2018/11/3.
  */
class ConfigurationEx(conf: Configuration) extends Logging {
  def getRequiredValueAsString(key: String): String = {
    getRequiredValue(key, (x) => x);
  }

  def getRequiredValueAsInt(key: String): Int = {
    getRequiredValue(key, (x) => x.toInt);
  }

  def getRequiredValueAsBoolean(key: String): Boolean = {
    getRequiredValue(key, (x) => x.toBoolean);
  }

  private def getRequiredValue[T](key: String, convert: (String) => T)(implicit m: Manifest[T]): T = {
    getValueWithDefault(key, () => throw new ArgumentRequiredException(key), convert);
  }

  private def getValueWithDefault[T](key: String, defaultValue: () => T, convert: (String) => T)(implicit m: Manifest[T]): T = {
    val opt = conf.getRaw(key);
    if (opt.isEmpty) {
      val value = defaultValue();
      logger.debug(s"no value set for $key, using default: $value");
      value;
    }
    else {
      val value = opt.get;
      try {
        convert(value);
      }
      catch {
        case e: java.lang.IllegalArgumentException =>
          throw new WrongArgumentException(key, value, m.runtimeClass);
      }
    }
  }

  def getValueAsString(key: String, defaultValue: String) =
    getValueWithDefault(key, () => defaultValue, (x: String) => x)

  def getValueAsClass(key: String, defaultValue: Class[_]) =
    getValueWithDefault(key, () => defaultValue, (x: String) => Class.forName(x))

  def getValueAsInt(key: String, defaultValue: Int) =
    getValueWithDefault[Int](key, () => defaultValue, (x: String) => x.toInt)

  def getValueAsBoolean(key: String, defaultValue: Boolean) =
    getValueWithDefault[Boolean](key, () => defaultValue, (x: String) => x.toBoolean)

  def getAsFile(key: String, baseDir: File, defaultValue: File) = {
    getValueWithDefault(key, () => defaultValue, { x =>
      val file = new File(x);
      if (file.isAbsolute)
        file;
      else
        new File(baseDir, x);
    });
  }
}

class ArgumentRequiredException(key: String) extends
  RuntimeException(s"argument required: $key") {

}

class WrongArgumentException(key: String, value: String, clazz: Class[_]) extends
  RuntimeException(s"wrong argument: $key, value=$value, expected: $clazz") {

}

object ConfigUtils {
  implicit def config2Ex(conf: Configuration) = new ConfigurationEx(conf);
}
