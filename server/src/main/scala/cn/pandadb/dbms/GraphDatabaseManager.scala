package cn.pandadb.dbms

import cn.pandadb.kernel.GraphService
import cn.pandadb.server.common.lifecycle.Lifecycle

trait GraphDatabaseManager extends Lifecycle{

  def getDatabase(name: String): GraphService;

  def createDatabase(name: String): GraphService;

  def shutdownDatabase(name: String): Unit;

  def deleteDatabase(name: String): Unit;

  def getAllDatabases(): Iterable[GraphService];
}

