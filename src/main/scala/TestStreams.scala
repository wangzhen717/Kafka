import java.util.Properties

import Streams.CheckIn
import io.circe.generic.auto._
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, Stores}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import scala.collection.mutable

object TestStreams extends App with CirceSupport {

  override def main(args: Array[String]): Unit = {
//    val builder = new StreamsBuilder()
//    val topology = builder.build()

    val builder = new StreamsBuilder()
    val source = builder.stream[String, CheckIn](Const.INPUT_TOPIC)
    source
      .selectKey((_, _) => "msg")
      .repartition
    val topology = builder.build()

    val checkInStoreBuilder = Stores
      .keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore(Const.STATE),
        Serdes.String,
        CirceSupport.toSerde[mutable.ArrayBuffer[CheckIn]])

    topology
//      .addSource("Source", Const.INPUT_TOPIC)
//      .addGlobalStore(
//        checkInStoreBuilder,
//        "Source",
//        String.deserializer(),
//        String.deserializer(),
//        Const.INPUT_TOPIC,
//        "GlobalStoreProcessor",
//        new ProcessorSupplier[String, String] {
//          override def get(): GlobalStoreProcessor = new GlobalStoreProcessor
//        })
      .addProcessor("TestCheckInProcessor", new ProcessorSupplier[String, CheckIn] {
        override def get(): TestCheckInProcessor = new TestCheckInProcessor
      }, "KSTREAM-SOURCE-0000000005")
      .addStateStore(checkInStoreBuilder, "TestCheckInProcessor")
      .addSink("Output", Const.OUTPUT_TOPIC, "TestCheckInProcessor")

    println(topology.describe())
    val streams = new KafkaStreams(topology, streamProperties())
    streams.cleanUp()
    streams.start()

    sys.addShutdownHook({
      streams.close()
    })
  }

  def streamProperties(): Properties = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    props
  }
}