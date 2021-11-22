package cn.pandadb.kernel.distribute.index.utils

import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.grapheco.lynx.{LynxBoolean, LynxDate, LynxDateTime, LynxDouble, LynxDuration, LynxInteger, LynxList, LynxLocalDateTime, LynxLocalTime, LynxNumber, LynxString, LynxTime}

import scala.collection.JavaConverters._
/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-22 15:57
 */
object IndexConverter {
  def transferType2Java(value: Any): Object ={
    value match {
      case n: LynxInteger => n.value.asInstanceOf[Object]
      case n: LynxDouble => n.value.asInstanceOf[Object]
      case n: LynxNumber => n.value.asInstanceOf[Object]
      case n: LynxString => n.value.asInstanceOf[Object]
      case n: LynxBoolean => n.value.asInstanceOf[Object]
      case n: LynxList => n.value.map(f => transferType2Java(f))
      case n: LynxDate => n.value.asInstanceOf[Object]
      case n: LynxDateTime => n.value.asInstanceOf[Object]
      case n: LynxLocalDateTime => n.value.asInstanceOf[Object]
      case n: LynxLocalTime => n.value.asInstanceOf[Object]
      case n: LynxTime => n.value.asInstanceOf[Object]
      case n: LynxDuration => n.value.asInstanceOf[Object]
      case n: Seq[Any] => seqAsJavaList(n.map(transferType2Java))
      case n => n.asInstanceOf[Object]
    }
  }
  def value2TermQuery(boolQueryBuilder: BoolQueryBuilder, key: String, value: Any): Unit ={
    value match {
      case n: List[Any] =>  n.foreach(f => value2TermQuery(boolQueryBuilder, key, f))
      case n: String => boolQueryBuilder.must(QueryBuilders.termQuery(s"$key.keyword", n).caseInsensitive(true))
      case n => boolQueryBuilder.must(QueryBuilders.termQuery(s"$key", n))
    }
  }
}
