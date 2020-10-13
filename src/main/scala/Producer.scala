import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random

object Producer {

  private val EVENT = 10

  def main(args: Array[String]): Unit = {
    val events = EVENT
    val topic = Const.TOPIC
    val brokers = Const.BROKEN
    val rnd = new Random()
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "Producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()
    for (_ <- Range(0, events)) {
      val key = "msg"
      val msg = rnd.nextInt() + ""
      val data = new ProducerRecord[String, String](topic, key, msg)
      producer.send(data)
    }

    System.out.println("sent: " + events)
    producer.close()
  }
}
