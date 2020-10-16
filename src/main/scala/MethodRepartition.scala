import java.io.{BufferedReader, File, FileReader}
import java.util.Properties

import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext, ProcessorSupplier}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object MethodRepartition extends App with CirceSupport {

  private var windowSize = 3
  private var styleFilePath = ""
  private var beerStyles: mutable.HashSet[String] = _

  override def main(args: Array[String]): Unit = {
    windowSize = args(0).toInt
    styleFilePath = args(1)
    beerStyles = loadBeerStyle(styleFilePath)

    val builder = new StreamsBuilder()
    val source = builder.stream[String, CheckIn](Const.INPUT_TOPIC)
    source
      .filter((_, checkIn) => {
        beerStyles.contains(checkIn.style)
      })
      .selectKey((_, _) => "msg")
      .repartition
    val topology = builder.build()

    val checkInStoreBuilder = Stores
      .keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore(Const.STATE),
        Serdes.String,
        CirceSupport.toSerde[mutable.ArrayBuffer[CheckIn]])

    topology
      .addProcessor("TestCheckInProcessor", new ProcessorSupplier[String, CheckIn] {
        override def get(): TestCheckInProcessor = new TestCheckInProcessor
      }, "KSTREAM-SOURCE-0000000006")
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

  class CheckInProcessor extends AbstractProcessor[String, CheckIn]{

    private val stateName = Const.STATE
    private var processorContext: ProcessorContext = _
    private var keyValueStore: KeyValueStore[String, mutable.ArrayBuffer[CheckIn]] = _
    private val storeKey = "AllHistoryCheckIn"

    override def init(context: ProcessorContext): Unit = {
      processorContext = context
      keyValueStore = processorContext.getStateStore(stateName).asInstanceOf[KeyValueStore[String, mutable.ArrayBuffer[CheckIn]]]
    }

    override def process(newKey: String, newCheckIn: CheckIn): Unit = {

      val currentTimeStamp = newCheckIn.timestamp

      if (keyValueStore.get(storeKey) == null) {
        keyValueStore.put(storeKey, new ArrayBuffer[CheckIn]())
      }
      val checks = keyValueStore.get(storeKey)
      checks.append(newCheckIn)
      val filterChecks = checks.filter(check => !inWindow(check.timestamp, windowSize, currentTimeStamp))
      keyValueStore.put(storeKey, filterChecks)
      println("+++++++++++++++++++++++++++++++++++++++++++++++++++")
      val updateMap = new mutable.HashMap[Long, Long]()
      filterChecks.foreach(checkIn => {
        if(!updateMap.contains(checkIn.city_id)) {
          updateMap.put(checkIn.city_id, 1)
        }
        else {
          val oldCount: Long = updateMap(checkIn.city_id)
          updateMap.put(checkIn.city_id, oldCount + 1)
        }
        println(checkIn)
      })
      println("---------------------------------------------------")
      val iter = updateMap.iterator
      while(iter.hasNext) {
        val update = iter.next()
        processorContext.forward("msg", Update(update._1, update._2).asJson.noSpaces)
      }
    }

    def inWindow(checkTimestamp: Long, window: Long, currentTimestamp: Long): Boolean = {
      checkTimestamp <= (currentTimestamp - window)
    }
  }

  def loadBeerStyle(styleFilePath: String): mutable.HashSet[String] = {
    val beerStyles = new mutable.HashSet[String]()
    val file = new BufferedReader(new FileReader(new File(styleFilePath)))
    var line = file.readLine()
    while(line != null) {
      beerStyles.add(line)
      line = file.readLine()
    }
    beerStyles
  }
}