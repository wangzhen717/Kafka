import java.io.{BufferedReader, File, FileReader}
import java.util.{Date, Properties}
import java.util.Properties

import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
import io.circe.generic.auto._
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import org.apache.kafka.clients.producer.internals.FutureRecordMetadata
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object Producer {

  private val EVENT = 5
  private val topic = Const.INPUT_TOPIC

  def main(args: Array[String]): Unit = {
    val events = EVENT
    val brokers = Const.BROKEN
    val rnd = new Random()
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "Producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val file = new BufferedReader(new FileReader(new File("C:\\Users\\billy\\Documents\\MyDocuments\\Assignment\\Supercomputing\\lab3\\info.json")))

    val producer = new KafkaProducer[String, String](props)

    var line = file.readLine()
    while(line != null) {
      val msg: Either[Error, CheckIn] = decode[CheckIn](line)
      val checkIn = msg.toSeq.head
      val key = checkIn.city_id + ""
      println(checkIn)
      val data = new ProducerRecord[String, String](topic, key, checkIn.asJson.noSpaces)
      producer.send(data)
      Thread.sleep(new Random().nextInt(400) + 100)
      line = file.readLine()
    }
    producer.close()
  }
}
