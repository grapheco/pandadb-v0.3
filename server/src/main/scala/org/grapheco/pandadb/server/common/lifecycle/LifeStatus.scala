package org.grapheco.pandadb.server.common.lifecycle

object LifeStatus extends Enumeration{
  type LifeStatus = Value

  val NONE = Value(0)
  val INITIALIZING = Value(1)
  val STARTING = Value(2)
  val STARTED = Value(3)
  val STOPPING = Value(4)
  val STOPPED = Value(5)
  val SHUTTING_DOWN = Value(6)
  val SHUTDOWN = Value(7)

}
