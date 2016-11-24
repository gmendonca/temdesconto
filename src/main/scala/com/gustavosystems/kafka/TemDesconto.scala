package com.gustavosystems.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.KafkaMessages.StringKafkaMessage
import kafka.serializer.{StringDecoder, StringEncoder}
import org.reactivestreams.{Publisher, Subscriber}
import com.softwaremill.react.kafka.{ConsumerProperties, ProducerProperties, ReactiveKafka}

/**
  * Created by gustavomendonca on 24/11/16.
  */
object TemDesconto {

  case class Config(zooKeeper: String = "localhost:2181",
                    topic: String = "",
                    brokerList: String = "localhost:9092",
                    groupId: String = "")

  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("Kafka Tem Desconto? Tem sim!", "1.0.0")

      opt[String]('z', "zooKeeper") action { (x, c) =>
        c.copy(zooKeeper = x) } text "zooKeeper host:port, otherwise localhost:2181 wil be used"

      opt[String]('t', "topic") action { (x, c) =>
        c.copy(topic = x) } text "Kafka topic name"

      opt[String]('t', "brokerList") action { (x, c) =>
        c.copy(brokerList = x) } text "Broker List"

      opt[String]('t', "groupId") action { (x, c) =>
        c.copy(groupId = x) } text "Kafka group Id"

      help("help") text "prints this usage text"
    }

    parser.parse(args, Config()) map { config =>
      implicit val actorSystem = ActorSystem("ReactiveKafka")
      implicit val materializer = ActorMaterializer()

      val kafka = new ReactiveKafka()
      val publisher: Publisher[StringKafkaMessage] = kafka.consume(ConsumerProperties(
        brokerList = config.brokerList,
        zooKeeperHost = config.zooKeeper,
        topic = config.topic,
        groupId = config.groupId,
        decoder = new StringDecoder()
      ))

      Source.fromPublisher(publisher).map(m => println("Kafka Tem Desconto? Tem sim ó: " + m.message()))

    } getOrElse {
      println("Do you know what you're doing? I don't think so!")
    }
  }

}
