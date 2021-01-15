package cn.pandadb.kernel.kv.db

trait KeyValueDB {
  def get(key: Array[Byte]): Array[Byte]
  def put(key: Array[Byte], value: Array[Byte]):Unit
  def write(option: Any, batch: Any):Unit // TODO option and batch trait
  def delete(key: Array[Byte]):Unit
  def deleteRange(key1: Array[Byte], key2: Array[Byte])
  def newIterator():KeyValueIterator
  def flush():Unit
  def close():Unit
}

trait KeyValueIterator {
  def isValid: Boolean
  def seek(key: Array[Byte]): Unit
  def seekToFirst():Unit
  def seekToLast():Unit
  def seekForPrev(key: Array[Byte]):Unit
  def next():Unit
  def prev():Unit
  def key():Array[Byte]
  def value():Array[Byte]
}

trait KeyValueWriteBatch {
  def put(key: Array[Byte], value: Array[Byte]):Unit
}
