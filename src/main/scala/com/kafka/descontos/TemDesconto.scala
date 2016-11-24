package com.kafka.descontos

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.softwaremill.react.kafka.KafkaMessages.StringKafkaMessage
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import kafka.serializer.StringDecoder
import org.reactivestreams.Publisher

import scala.concurrent.Future

/**
  * Created by gustavomendonca on 24/11/16.
  */
object TemDesconto extends {

  case class Config(zooKeeper: String = "localhost:2181",
                    topic: String = "",
                    brokerList: String = "localhost:6667",
                    groupId: String = "")

  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[Config]("scopt") {
      head("Kafka tem desconto? Tem sim!", "1.0.0")

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

      import actorSystem.dispatcher

      val consumerProperties = ConsumerProperties(
        brokerList = config.brokerList,
        zooKeeperHost = config.zooKeeper,
        topic = config.topic,
        groupId = config.groupId,
        decoder = new StringDecoder()
      )

      val kafka = new ReactiveKafka()

      val consumer = kafka.consumeWithOffsetSink(consumerProperties)

      Source.fromPublisher(consumer.publisher).mapAsync(1)(m =>{
        println("Kafka tem desconto? Tem sim ó: " + m.message())
        Future(m)
      }).runWith(consumer.offsetCommitSink)

    } getOrElse {
      println("Do you know what you're doing? I don't think so!")
    }
  }

}
