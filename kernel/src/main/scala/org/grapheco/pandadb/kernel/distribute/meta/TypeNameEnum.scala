package org.grapheco.pandadb.kernel.distribute.meta

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-16 09:25
 */
object TypeNameEnum extends Enumeration {
    type TypeNameEnum = Value
    val nodeName: Value = Value(1)
    val relationName: Value = Value(2)
}
