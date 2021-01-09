package org.opencypher.lynx.planning

import cn.pandadb.kernel.store.StoredValue
import org.opencypher.okapi.api.types.CypherType

import scala.collection.Seq

class LinTable(val schema:Seq[String], val recordes: Iterator[Seq[_ <: StoredValue]], val schemas: Seq[(String, CypherType)]) {
  private lazy val _columnIndex = schema.zipWithIndex.toMap

  def cell(row: Seq[_ <: StoredValue], column: String): StoredValue =
    row(_columnIndex(column))
}
