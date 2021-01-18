//package cn.pandadb.kernel.optimizer.simulator
//
//import cn.pandadb.kernel.kv.node.NodeStore
//import cn.pandadb.kernel.kv.relation.RelationStoreAPI
//
//class ExpandOrJoinSimulator(nodeApi: NodeStore, relApi: RelationStoreAPI, leftLabelId:Int=1, rightLabelId:Int=2, relationTypeId:Int=1){
//  //cypher: String = "match (n:label1)->[r:type1]->(m: label2) return count(m)"
//  def OnTheFlyExpandNodeFirst(leftFirst: Boolean = true): Long ={
//    var result: Long = 0
//    if(leftFirst){
//      nodeApi.getNodeIdsByLabel(leftLabelId).map(nodeId => {
//        relApi.findOutRelations(nodeId, relationTypeId).map(r=>{
//          if (!nodeApi.get(r.to, rightLabelId).isEmpty){
//            result += 1
//          }
//        })
//      })
//    }
//    else{
//      nodeApi.getNodeIdsByLabel(rightLabelId).map(nodeId => {
//        relApi.findInRelations(nodeId, relationTypeId).map(r=>{
//          if (!nodeApi.get(r.from, leftLabelId).isEmpty){
//            result += 1
//          }
//        })
//      })
//    }
//    result
//  }
//  def OnTheFlyExpandRelationFirst(): Long ={
//    var result: Long = 0
//    relApi.getRelationIdsByRelationType(relationTypeId).map(rId=>{
//      val r = relApi.getRelationById(rId).get
//      if(!nodeApi.get(r.from, leftLabelId).isEmpty && !nodeApi.get(r.to, rightLabelId).isEmpty){
//        result+=1
//      }
//    })
//    result
//  }
//
//  def PreloadThenJoin(): Long ={
//    var result: Long = 0
//    val leftNodeIds = nodeApi.getNodeIdsByLabel(leftLabelId)
//    val rightNodeIds = nodeApi.getNodeIdsByLabel(rightLabelId)
//    val rels = relApi.getRelationIdsByRelationType(relationTypeId).map(rid => {
//      relApi.getRelationById(rid).get
//    })
//    //here we should choose the larger nodeIds to map at first, but we cannot get the iterator length. so choose left
//    // for test
//    val wideTable = leftNodeIds.flatMap(lid => {
//      rels.filter(_.from==lid).map(r=>{
//        r.to -> r.from
//      })
//    })
//    rightNodeIds.map(rid=>{
//      if(wideTable.filter(_._1==rid).isEmpty){
//        result+=1
//      }
//    })
//    result
//  }
//
//}
