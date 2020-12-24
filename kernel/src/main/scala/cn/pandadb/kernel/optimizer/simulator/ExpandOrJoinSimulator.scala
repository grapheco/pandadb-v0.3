//package cn.pandadb.kernel.optimizer.simulator
//
//import cn.pandadb.kernel.kv.RocksDBGraphAPI
//
//class ExpandOrJoinSimulator(api: RocksDBGraphAPI, leftLabelId:Int=1, rightLabelId:Int=2, relationTypeId:Int=1){
//  //cypher: String = "match (n:label1)->[r:type1]->(m: label2) return count(m)"
//  def OnTheFlyExpandNodeFirst(leftFirst: Boolean = true): Long ={
//    var result: Long = 0
//    if(leftFirst){
//      api.findNodes(leftLabelId).map(nodeId => {
//        api.findOutRelations(nodeId, relationTypeId).map(r=>{
//          if (api.nodeAt(r.to).labelIds.contains(rightLabelId)){
//            result += 1
//          }
//        })
//      })
//    }
//    else{
//      api.findNodes(rightLabelId).map(nodeId => {
//        api.findInRelations(nodeId, relationTypeId).map(r=>{
//          if (api.nodeAt(r.from).labelIds.contains(leftLabelId)){
//            result += 1
//          }
//        })
//      })
//    }
//    result
//  }
//  def OnTheFlyExpandRelationFirst(): Long ={
//    var result: Long = 0
//    api.getRelationsByType(relationTypeId).map(rId=>{
//      val r = api.relationAt(rId)
//      if(api.nodeAt(r.from).labelIds.contains(leftLabelId) &&
//        api.nodeAt(r.to).labelIds.contains(rightLabelId)){
//        result += 1
//      }
//    })
//    result
//  }
//
//  def PreloadThenJoin(): Long ={
//    var result: Long = 0
//    val leftNodeIds = api.findNodes(leftLabelId).toArray[Long]
//    val rightNodeIds = api.findNodes(rightLabelId).toArray[Long]
//    api.getRelationsByType(relationTypeId).map(rId=>{
//      val r = api.relationAt(rId)
//      if(leftNodeIds.contains(r.from) && rightNodeIds.contains(r.to)){
//        result += 1
//      }
//    })
//    result
//  }
//
//}
