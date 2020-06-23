package flink.embedded.elasticsearch.examples

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.elasticsearch.client.Requests
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import scala.util.Random

/**
 * 模拟数据写入Es
 *
 * @author shirukai
 */
object WriteElasticSearchJob {

  case class Event(id: String, v: Double, t: Long)

  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 解析参数
    val params: ParameterTool = ParameterTool.fromArgs(args)

    // 1. 从集合中创建一个DataStream
    val events = env.fromCollection(1 to 20).map(i => {
      val v = Random.nextDouble()
      val t = System.currentTimeMillis()
      Event("event-" + i, v, t)
    })

    // 2. 将事件写入Es
    val (userConfig, transportAddresses) = parseEsConfigs(params)
    import scala.collection.JavaConversions._
    val esSink = new ElasticsearchSink(userConfig, transportAddresses, new EventSinkFunction)
    events.addSink(esSink)

    env.execute("WriteElasticSearchJob")

  }

  def parseEsConfigs(params: ParameterTool): (Map[String, String], List[InetSocketAddress]) = {
    // 构建userConfig，主要设置Es集群名称
    val userConfig = Map[String, String](
      "cluster.name" -> params.get("es.cluster.name", "es-test")
    )
    // 构建transport地址
    val esNodes = params.get("es.cluster.nodes", "127.0.0.1").split(",").toList
    val esPort = params.getInt("es.cluster.port", 9300)
    val transportAddresses = esNodes.map(node => new InetSocketAddress(InetAddress.getByName(node), esPort))
    (userConfig, transportAddresses)
  }

  /**
   * 继承ElasticsearchSinkFunction实现构建索引的方法
   */
  class EventSinkFunction extends ElasticsearchSinkFunction[Event] {
    override def process(t: Event, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
      implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
      val source: String = Serialization.write(t)
      requestIndexer.add(Requests.indexRequest()
        .index("events")
        .`type`("test")
        .id(t.id)
        .source(source)
      )
    }
  }

}
