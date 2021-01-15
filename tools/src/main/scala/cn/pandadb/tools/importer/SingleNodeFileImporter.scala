package cn.pandadb.tools.importer

import cn.pandadb.kernel.PDBMetaData

import java.io.File

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:20 2021/1/15
 * @Modified By:
 */
class SingleNodeFileImporter(file: File, importCmd: ImportCmd) extends SingleFileImporter {
//  override val csvIter: Iterator[CSVLine] = new CSVReader(file, importCmd.delimeter).getAsCSVLines
  override val importerFileReader: ImporterFileReader = new ImporterFileReader(file, importCmd.delimeter)
  override val headLine: Array[String] = importerFileReader.getHead.getAsArray
  override val idIndex: Int = headLine.indexWhere(item => item.contains(":ID"))
  override val labelIndex: Int = headLine.indexWhere(item => item.contains(":LABEL"))
  override val propHeadMap: Map[Int, (Int, String)] = {
    val buffer = headLine.toBuffer
    buffer.remove(idIndex)
    buffer.remove(labelIndex)
    buffer.zipWithIndex.map(item => {
      val pair = item._1.split(":")
      val propId = PDBMetaData.getPropId(pair(0))
      val propType = {
        if (pair.length == 2) pair(1).toLowerCase()
        else "string"
      }
      (item._2, (propId, propType))
    }).toMap
  }
}
