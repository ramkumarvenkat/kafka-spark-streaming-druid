package pipeline.kafka

import java.util.Date
import kafka.producer.Producer
import java.util.Properties
import kafka.producer.ProducerConfig
import scala.util.Random
import kafka.producer.KeyedMessage
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.producer.KeyedMessage
import java.util.concurrent.ThreadPoolExecutor
import pipeline.model.avro.KafkaEvent
import pipeline.common.SerDeUtil
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import java.io.FileReader
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import pipeline.config.InputConfig
import scala.io.Source;
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

object KafkaTasks {

  def main(args: Array[String]): Unit = {
    if (args.length == 2) {

      val reader = new FileReader(args(1))
      val mapper = new ObjectMapper(new YAMLFactory())
      val config: InputConfig = mapper.readValue(reader, classOf[InputConfig])

      KafkaTasks.produce(args(0), config)
      //KafkaTasks.consume() 
    }
  }

  def produce(file: String, inputConfig: InputConfig) = {

    val props = new Properties()
    props.put("metadata.broker.list", "10.24.48.147:6092")
    props.put("value.serializer.class", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("key.serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "sync")
    val config = new ProducerConfig(props)

    val producer = new Producer[String, Array[Byte]](config)

    val rnd = new Random()

    val bufferedSource = Source.fromFile(file)

    val lines = bufferedSource.getLines

    val fields = new ArrayBuffer[String]

    //First entry is timestamp, needed for Druid indexing
    fields += "timestamp"

    //First line contains fieldnames
    lines.next().split(",").foreach(field => fields += field)

    for (line <- lines) {
      var values = line.split(",")

      //Ading currenttime as the first element
      values +:= DateTime.now().toString(ISODateTimeFormat.dateTime())

      val eventDataMap = (fields.toList, values.toList).zipped.toMap.collect {
        case (k, v) if !(Option(v).getOrElse("").trim.isEmpty) => (k, v)
      }

      var kafkaEvent = new KafkaEvent()
      kafkaEvent.setData(eventDataMap.asJava)

      println("Producing : " + kafkaEvent)

      val data = new KeyedMessage[String, Array[Byte]](inputConfig.KAFKA.TOPIC, null, SerDeUtil.serializeEvent(kafkaEvent))
      producer.send(data)

      Thread.sleep(1000)
    }

    bufferedSource.close()
    producer.close()
  }

  def consume() = {

    val props = new Properties()
    props.put("zookeeper.connect", "10.24.48.147:2181");
    props.put("group.id", "test-consumer");
    props.put("zookeeper.session.timeout.ms", "4000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    val config = new ConsumerConfig(props)

    val consumer = Consumer.create(config)

    val consumerMap = consumer.createMessageStreams(Map[String, Int]("test" -> 1))
    val stream = consumerMap.get("test").get.apply(0)

    val message = stream.foreach(streamMessage => {
      val message = SerDeUtil.deserialiseEvent(streamMessage.message)
      println("Consuming : " + message)
    })

    consumer.shutdown()
  }
}