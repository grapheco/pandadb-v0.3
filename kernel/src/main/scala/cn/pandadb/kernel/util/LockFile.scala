package cn.pandadb.kernel.util

import java.io.{File, FileInputStream, FileOutputStream}
import java.lang.management.ManagementFactory

import org.apache.commons.io.IOUtils

class LockFile(lockFile: File) {
  def assertUnlocked(): Unit = {
    if (lockFile.exists()) {
      val fis = new FileInputStream(lockFile)
      val pid = IOUtils.toString(fis).toInt
      fis.close()

      throw new LockFileExistedException(lockFile, pid)
    }
  }

  def lock(): Unit = {
    val pid = ProcessUtils.getCurrentPid()
    val fos = new FileOutputStream(lockFile)
    fos.write(pid.toString.getBytes())
    fos.close()
  }

  def unlock(): Unit = {
    lockFile.delete()
  }
}

object ProcessUtils {
  def getCurrentPid(): Int = {
    val runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    runtimeMXBean.getName().split("@")(0).toInt;
  }
}

class LockFileExistedException(lockFile: File, pid: Int) extends RuntimeException {

}
