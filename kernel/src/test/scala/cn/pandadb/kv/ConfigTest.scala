package cn.pandadb.kv

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.Properties

import org.junit.Assert

object ConfigTest {
  def main(args: Array[String]): Unit = {
    val is = new BufferedInputStream(new FileInputStream(new File("./kernel/rocksdb.conf")))
    val prop = new Properties()
    prop.load(is)
    Assert.assertEquals(classOf[Boolean], prop.getProperty("rocksdb.isHDD").toBoolean.getClass)
  }
}
