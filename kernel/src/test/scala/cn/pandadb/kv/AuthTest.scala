package cn.pandadb.kv

import java.io.File

import cn.pandadb.kernel.kv.ByteUtils
import cn.pandadb.kernel.kv.meta.Auth
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}
import org.junit.Assert._

/**
 * @ClassName AuthTest
 * @Description TODO
 * @Author huchuan
 * @Date 2021/1/19
 * @Version 0.1
 */
class AuthTest {

  var auth:Auth = _
  val path = "./testdata"

  @Before
  def init(): Unit ={
    FileUtils.deleteDirectory(new File("./testdata"))
    auth = new Auth(path)
  }

  @Test
  def justOneTest(): Unit ={
    // login use "pandadb":"pandadb" as a new user
    assertTrue(auth.isDefault)
    assertTrue(auth.check("pandadb", "pandadb"))
    // change password
    assertTrue(auth.set("pandadb","123"))
    // now can't use default token
    assertFalse(auth.isDefault)
    assertFalse(auth.check("pandadb", "pandadb"))
    assertTrue(auth.check("pandadb", "123"))
    // add a new user
    assertTrue(auth.add("newUser", "123"))
    // check it
    assertTrue(auth.check("newUser", "123"))
    // add again, it will return false
    assertFalse(auth.add("newUser", "123"))
    // delete pandadb
    assertTrue(auth.delete("pandadb"))
    // check pandadb
    assertFalse(auth.check("pandadb", "123"))
    // delete again, it will return false
    assertFalse(auth.delete("pandadb"))
    // add another one with empty password
    assertTrue(auth.add("user2", ""))
    assertTrue(auth.check("user2", ""))
    // show all user
    auth.all().foreach(println)
    assertFalse(auth.isDefault)
  }
}
