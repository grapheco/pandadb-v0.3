package cn.pandadb.driver.test

import cn.pandadb.driver.PandaAuthToken
import cn.pandadb.driver.utils.Profiler
import cn.pandadb.kernel.util.Profiler
import org.neo4j.driver.{Driver, GraphDatabase}


object Graph500Test {

  val driver:Driver  = GraphDatabase.driver("127.0.0.1:8878", PandaAuthToken.basic("panda", "db"))
  val session = driver.session()

  def main(args: Array[String]): Unit = {
    println("match (n:label0)-[r]->(m:label1) return r  limit 10000")
    Profiler.timing({
      val res = session.run("match (n:label0)-[r]->(m:label1) return r  limit 10000")
      println(res.stream().count())
    })

//
//    println("match (n:label2)-[r1]->(m:label3)-[r2]->(p:label4) return r2  limit 10000")
//    Profiler.timing({
//      val res = session.run("match (n:label2)-[r1]->(m:label3)-[r2]->(p:label4) return r2  limit 10000")
//      println(res.stream().count())
//    })
//
//    println("match (n:label5)-[r1]->(m:label6)-[r2]->(p:label7)-[r3]->(q:label8) return r3  limit 10000")
//    Profiler.timing({
//      val res = session.run("match (n:label5)-[r1]->(m:label6)-[r2]->(p:label7)-[r3]->(q:label8) return r3  limit 10000")
//      println(res.stream().count())
//    })
//
//    println("match (n:label0)-[r:type0]->(m:label2) return r  limit 10000")
//    Profiler.timing({
//      val res = session.run("match (n:label0)-[r:type0]->(m:label2) return r  limit 10000")
//      println(res.stream().count())
//    })
//
//    println("match (n:label2)-[r]->(m:label4) return r  limit 10000")
//    Profiler.timing({
//      val res = session.run("match (n:label2)-[r]->(m:label4) return r  limit 10000")
//      println(res.stream().count())
//    })
//
//    driver.close()
  }

}
