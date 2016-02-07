package pipeline.common

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

class MyKyroRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[pipeline.model.avro.KafkaEvent])
  }
}