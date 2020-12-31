package cn.pandadb.optimizer

import org.junit.Test

class PandaGraphTest {

  @Test
  def testGetNodes(): Unit = {
    val labels: Map[String, Int] = Map("test"->100, "king"->200, "kl"->10)
    val si = labels.toArray.sortBy(_._2).head._2
    println(si)
  }

  @Test
  def testGnodes(): Unit = {
    val labels:Array[String] = Array("blob", "alex", "simba")
    val cnts: Array[Int] = Array(1,4,6,7,8,9)
    val sd1 = labels.flatMap(x => {
      cnts.map(u => x -> u)
    })
    val sd2 = labels.flatMap(x => {
      cnts.flatMap(u => {
        cnts.map(y => u ->y)
      }).filter(p =>p._1 !=p._2).map(g => x-> g)
    })

    val sd4 = cnts.flatMap(u => {
      cnts.map(y => Set(u, y))
    }).toSet

    val sd3 = labels.flatMap(x => {
      cnts.map(u => {
        cnts.map(y => Set(u, y))
      }).toSet
    })

    val sd5 = labels.map(x => {
      sd4.map(g => x->g)
    }).flatten

    sd1
    sd2
    sd3
    sd4



  }

  @Test
  def testSet(): Unit ={
    val s1 = Set("test",2)
    val s2 = Set(2,"hjk")
    val s3 = Set(2,"test")
    val s4 = Set(s1,s2,s3)
    s4
  }

}
