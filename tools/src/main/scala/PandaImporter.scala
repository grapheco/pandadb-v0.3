import java.io.File
import org.rocksdb.RocksDB

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 15:03 2020/12/3
 * @Modified By:
 */

trait PandaImporter {
  val db: RocksDB;
  val file: File;
  val headFile: File;
  val headMap: Map[Int, String]
}
