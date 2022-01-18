package org.grapheco.pandadb.server.common.lifecycle

import org.grapheco.pandadb.server.common.lifecycle.LifeStatus.LifeStatus

import scala.collection.mutable.ArrayBuffer

class LifecycleSupport extends Lifecycle {
  private val components = ArrayBuffer[Lifecycle]()
  private  var status = LifeStatus.NONE

  override def init(): Unit =  {
    if (status == LifeStatus.NONE) {
      status = changedStatus(status, LifeStatus.INITIALIZING)
      components.foreach(component => {
        component.init()
      })
      status = changedStatus(status, LifeStatus.STOPPED)
    }
  }

  override def start(): Unit = {
    init()
    if (status == LifeStatus.STOPPED) {
      status = changedStatus(status, LifeStatus.STARTING)
      components.foreach(component => {
        component.start()
      })
      status = changedStatus(status, LifeStatus.STARTED)
    }
  }

  override def stop(): Unit =  {
    if (status == LifeStatus.STARTED) {
      status = changedStatus(status, LifeStatus.STOPPING)
      components.foreach(component => {
        component.stop()
      })
      status = changedStatus(status, LifeStatus.STOPPED)
    }
  }

  override def shutdown(): Unit =  {
    stop()
    if (status == LifeStatus.STOPPED) {
      status = changedStatus(status, LifeStatus.SHUTTING_DOWN)
      components.foreach(component => {
        component.shutdown()
      })
      status = changedStatus(status, LifeStatus.SHUTDOWN)
    }
  }

  def add[T <: Lifecycle](instance: T): T = this.synchronized {
    components.append(instance)
    instance
  }

  def remove(instance: Lifecycle): Boolean = this.synchronized {
    for (i <- 0 to components.size-1) {
      if (components(i) == instance) {
        val tmp = components.remove(i)
        tmp.shutdown()
        true
      }
    }
    false
  }

  private def changedStatus(oldStatus: LifeStatus, newStatus: LifeStatus): LifeStatus = {
    newStatus
  }
}
