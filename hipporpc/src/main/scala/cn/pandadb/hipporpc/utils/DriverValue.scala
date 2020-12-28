package cn.pandadb.hipporpc.utils

import cn.pandadb.hipporpc.values.Value

/*
    eg: match (n) return n, n.name, n.age
    a row result (n, n.name, n.age) is represented by DriverValue
 */
case class DriverValue(rowMap:Map[String, Value]){

}
