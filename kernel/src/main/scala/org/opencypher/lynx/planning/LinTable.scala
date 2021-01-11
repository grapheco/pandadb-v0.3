package org.opencypher.lynx.planning

import cn.pandadb.kernel.store.StoredValue
import org.opencypher.okapi.api.types.CypherType

import scala.collection.Seq

class LinTable(val schema:Seq[String], val recordes: Iterator[Seq[_ <: StoredValue]], val schemas: Seq[(String, CypherType)], val reSchemas: Seq[(String, Boolean)]) {
  private lazy val _columnIndex = schema.zipWithIndex.toMap

  def cell(row: Seq[_ <: StoredValue], column: String): StoredValue =
    row(_columnIndex(column))
  def selectRecordes():Iterator[Seq[_ <: StoredValue]] = {
    recordes.map(row => {
      reSchemas.filter(_._2).map(str => cell(row, str._1))
    })
  }

  def selectSchema(): Seq[(String, CypherType)] = {
    val sStr = reSchemas.filter(_._2).map(_._1)
    schemas.filter(tup => sStr.contains(tup._1))
  }
}
