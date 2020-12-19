package cn.pandadb.kernel.util.serializer

import com.twitter.chill.{Input, KryoBase, Output, ScalaKryoInstantiator}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:24 2020/12/17
 * @Modified By:
 */
object ChillSerializer {

  val kryo: KryoBase = {
    val instantiator = new ScalaKryoInstantiator()
    instantiator.setRegistrationRequired(true)
    instantiator.newKryo
  }

  def serialize(obj: Any): Array[Byte] = {
    val bytesArr = new Array[Byte](4096)
    val output = new Output(bytesArr)
    kryo.writeObject(output, obj)
    output.flush()
    output.close()
    bytesArr
  }

  def deserialize[T](bytesArr: Array[Byte], t: Class[T]): T = {
    val input = new Input(bytesArr)
    kryo.readObject(input, t)
  }
}
