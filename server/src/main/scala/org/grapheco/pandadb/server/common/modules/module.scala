package org.grapheco.pandadb.server.common.modules

import org.grapheco.pandadb.server.common.configuration.Config
import org.grapheco.pandadb.server.common.lifecycle.LifecycleAdapter


trait LifecycleServerModule extends LifecycleAdapter {
}

trait LifecycleServer extends LifecycleAdapter {
  def getConfig(): Config
}

