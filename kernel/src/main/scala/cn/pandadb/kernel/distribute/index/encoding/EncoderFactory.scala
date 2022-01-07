package cn.pandadb.kernel.distribute.index.encoding

import cn.pandadb.kernel.distribute.index.PandaDistributedIndexStore
import cn.pandadb.kernel.distribute.index.encoding.encoders.{IndexEncoderNames, TreeEncoder}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-01-07 09:11
 */
object EncoderFactory {
  def getEncoder(name: String, indexStore: PandaDistributedIndexStore): IndexEncoder ={
    name match {
      case IndexEncoderNames.treeEncoder => new TreeEncoder(indexStore)
    }
  }
}


trait IndexEncoder{

}