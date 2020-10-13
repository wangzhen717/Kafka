import java.time.Duration
import java.util.concurrent._
import java.util.{Collections, Properties}
import scala.collection.JavaConversions._

import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

class ScalaConsumerExample(val brokers: String,
                           val groupId: String,
                           val topic: String) extends Logging {

  private val props = createConsumerConfig(brokers, groupId)
  val consumer = new KafkaConsumer[String, String](props)
  var executor: ExecutorService = null

  def shutdown(): Unit = {
    if (consumer != null)
      consumer.close();
    if (executor != null)
      executor.shutdown();
  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run(): Unit = {
    consumer.subscribe(Collections.singletonList(this.topic))

    Executors.newSingleThreadExecutor.execute(() => {
      while (true) {
        val records = consumer.poll(Duration.ofMillis(0))

        for (record <- records) {
          System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
        }
      }
    })
  }
}

object Consumer extends App {

  val example = new ScalaConsumerExample(Const.BROKEN, Const.GROUPID, Const.TOPIC)
  example.run()
}