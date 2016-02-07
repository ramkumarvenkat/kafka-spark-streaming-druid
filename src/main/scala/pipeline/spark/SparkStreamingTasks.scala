package pipeline.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import pipeline.common.SerDeUtil
import kafka.serializer.DefaultDecoder
import kafka.serializer.DefaultDecoder
import org.apache.spark.storage.StorageLevel
import com.metamx.tranquility.spark.BeamRDD._
import pipeline.druid.EventRDDBeamFactory
import scala.collection.immutable.Map
import pipeline.model.avro.KafkaEvent
import org.apache.spark.rdd.RDD
import pipeline.druid.EventRDDBeamFactory
import java.util.HashMap
import scala.collection.JavaConversions._
import pipeline.config.InputConfig
import pipeline.config.InputConfig
import org.apache.spark.streaming.dstream.DStream
import pipeline.druid.EventRDDBeamFactory
import org.apache.spark.SparkContext

object SparkStreamingTasks {

  def startSparkContext(config: InputConfig) = {
    StreamingContext.getOrCreate(config.SPARK.CHECKPOINT, () => createSparkStreamingContext(config))
  }
  
  def createSparkStreamingContext(config: InputConfig) = {
    
    val sparkConf = new SparkConf()
      .setAppName(config.SPARK.APPNAME)
      .setMaster(config.SPARK.MASTER)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "pipeline.common.MyKyroRegistrator")
      
    val sparkContext = new SparkContext(sparkConf)
    val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(config.SPARK.DURATION_S))
    sparkStreamingContext.checkpoint(config.SPARK.CHECKPOINT)
    
    val dStream = getStreamFromKafka(sparkStreamingContext, config)
    val deserialisedDStream = deserialiseAvroObjectFromStream(dStream)
    processDStream(deserialisedDStream, config)

    sparkStreamingContext
  }
  
  def getStreamFromKafka(ssc: StreamingContext, config: InputConfig) = {

    val KAFKA_CONF = new HashMap[String,String]
    KAFKA_CONF.put("metadata.broker.list", config.KAFKA.BROKER)
    KAFKA_CONF.put("zookeeper.connect", config.ZOOKEEPER)
    KAFKA_CONF.put("group.id", config.SPARK.KAFKA.GROUP)

    config.SPARK.KAFKA.PROPERTIES.toList.foreach(property => KAFKA_CONF.put(property.PROPERTY, property.VALUE))

    val lines = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, KAFKA_CONF.toMap, Set(config.KAFKA.TOPIC))
    val raw = lines.map(_._2)
    raw
  }
  
  def deserialiseAvroObjectFromStream(dStream: DStream[Array[Byte]]) = {
    dStream.map(byteStream => SerDeUtil.deserialiseEvent(byteStream))
  }
  
  def processDStream(dStream: DStream[KafkaEvent], config: InputConfig) = {
    
    dStream foreachRDD { kafkaEventRDD =>
      { 
        val druidEventRDD = kafkaEventRDD map { kafkaEvent =>
          {
            kafkaEvent.getData().toMap
          }
        }
        
        druidEventRDD.propagate(new EventRDDBeamFactory(config))
      }
    }
    
  }
}