package cn.pandadb.server.common.modules

import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.lifecycle.LifecycleAdapter


trait LifecycleServerModule extends LifecycleAdapter {
}

trait LifecycleServer extends LifecycleAdapter {
  def getConfig(): Config
}

