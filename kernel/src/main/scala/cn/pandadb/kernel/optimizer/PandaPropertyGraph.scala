package cn.pandadb.kernel.optimizer

import cn.pandadb.kernel.kv.NFPredicate
import org.opencypher.lynx.LynxRecords
import org.opencypher.lynx.graph.LynxPropertyGraph
import org.parboiled.scala.utils.Predicate

trait PandaPropertyGraph extends LynxPropertyGraph{

  def isPropertyWithIndex(propertyName: String): Boolean = ???

  def isLabelWithIndex(label: String): Boolean = ???

  def isPopertysWithIndex(propertyName1: String, propertyName2: String): Boolean = ???

  def isLabelAndPropertyWithIndex(propertyName: String, label: String): Boolean = ???

  def getRecorderNumbersFromProperty(propertyName: String): Int = ???

  def getRecorderNumbersFromLabel(label: String): Int = ???

  def getRecorderNumberFromPredicate(predicate: NFPredicate): Int = ???

  def isNFPredicateWithIndex(predicate: NFPredicate): Boolean = ???

  def isNFPredicatesWithIndex(predicate: Array[NFPredicate]): Boolean = ???

  def getNodesByFilter(predicate: NFPredicate): LynxRecords = ???

}
