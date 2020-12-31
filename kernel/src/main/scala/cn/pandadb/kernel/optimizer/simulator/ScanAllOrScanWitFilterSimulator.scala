package cn.pandadb.kernel.optimizer.simulator

import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.kernel.kv.index.IndexStoreAPI

class ScanAllOrScanWitFilterSimulator(api: GraphFacadeWithPPD, indexApi: IndexStoreAPI, nodeLabelId:Int=1, nodeAttribution:(Int, Any)=(3, "1")){
  //cypher: String = "match (n) where n.flag=true return count(n)"
  //cypher: String = "match (n) where n.idStr=1 return count(n)"
  def ScanThenFilter(): Long ={
    var result: Long = 0
    api.allNodes().map(n => {
      if(n.properties.get(nodeAttribution._1)==nodeAttribution._2){
        result+=1
      }
    })
    result
  }

  def ScanWitFilter(): Long ={
    val indexId = indexApi.getIndexId(nodeLabelId, Array(nodeAttribution._1)).get
    indexApi.find(indexId, nodeAttribution._2).length
  }

}
