import Streams.CheckIn
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TestCheckInProcessor extends AbstractProcessor[String, CheckIn]{

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
    val filterChecks = checks.filter(check => !inWindow(check.timestamp, Const.WINDOW, currentTimeStamp))
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

case class Update (city_id: Long, count: Long)
