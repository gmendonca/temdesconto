package com.kafka.descontos

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.softwaremill.react.kafka.KafkaMessages.StringKafkaMessage
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import kafka.serializer.StringDecoder
import org.json4s.DefaultFormats
import org.reactivestreams.Publisher
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization._

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scalaj.http.{Http, HttpResponse}

/**
  * Created by gustavomendonca on 24/11/16.
  */
object TemDesconto extends {

  case class Config(zooKeeper: String = "localhost:2181",
                    topic: String = "",
                    brokerList: String = "localhost:6667",
                    groupId: String = "",
                    discount: Double = 0.3,
                    search: Seq[String] = List[String](""))

  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[Config]("scopt") {
      head("Kafka tem desconto? Tem sim!", "1.0.0")

      opt[String]('z', "zooKeeper") action { (x, c) =>
        c.copy(zooKeeper = x) } text "zooKeeper host:port, otherwise localhost:2181 wil be used"

      opt[String]('t', "topic") action { (x, c) =>
        c.copy(topic = x) } text "Kafka topic name"

      opt[String]('b', "brokerList") action { (x, c) =>
        c.copy(brokerList = x) } text "Broker List"

      opt[String]('g', "groupId") action { (x, c) =>
        c.copy(groupId = x) } text "Kafka group Id"

      opt[Double]('d', "discount") action { (x, c) =>
        c.copy(discount = x) } text "Minimum discount"

      opt[Seq[String]]('s', "search") action { (x, c) =>
        c.copy(search = x) } text "Minimum discount"

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
      ).readFromEndOfStream()

      val kafka = new ReactiveKafka()

      val consumer = kafka.consumeWithOffsetSink(consumerProperties)

      implicit val formats = DefaultFormats

      val raasUrl = "platform.chaordicsystems.com"

      Source.fromPublisher(consumer.publisher).mapAsync(1)(m => {
        val json = parse(m.message())

        val entity = json \ "entityUpdates"

        Try(entity.children.foreach(x => if ((x \ "attribute").extract[String] == "price") {

          val oldValue = (x \ "oldValue").extractOrElse[String]((x \ "oldValue").extract[Double] toString) toFloat
          val newValue = (x \ "newValue").extractOrElse[String]((x \ "newValue").extract[Double] toString) toFloat

          if ((oldValue - newValue) >= config.discount * oldValue && oldValue > 0 && newValue > 0) {

            val apiKey = (json \ "apiKey").extract[String]
            val version = (json \ "version").extract[String] toLowerCase
            val productId = (json \ "productId").extract[String]

            val url = s"https://$raasUrl/raas/$version/clients/$apiKey/products/$productId"

            val response:HttpResponse[String] = Http(url).auth(User.name, User.password).asString

            val product = parse(response.body)

            val productName = (product \ "name").extract[String]

            val it = config.search.iterator

            for (elem <- it.takeWhile(p => response.body.toLowerCase.contains(p))) {
              //if(response.body.toLowerCase.contains(elem)) {
              if(productName.toLowerCase.contains(elem)) {
                println("Kafka tem desconto? Tem sim ó: ")
                println("Loja = " + apiKey)
                println("Id do produto = " + productId)
                println("Nome do produto = " + productName)

                println("Preço antigo = " + (x \ "oldValue").extract[String])
                println("Preço novo = " + (x \ "newValue").extract[String])

                println()
                println("=====================================================================================")
                println()

              }
            }
          }
        })) match {
          case Success(v) => v
          case Failure(ex) => println(s"Couldn't do it ${ex.getMessage}")
        }
        Future(m)
      }).runWith(consumer.offsetCommitSink)


    } getOrElse {
      println("Do you know what you're doing? I don't think so!")
    }
  }

}
