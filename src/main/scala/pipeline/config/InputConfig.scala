package pipeline.config

import com.fasterxml.jackson.annotation.JsonProperty

@SerialVersionUID(1L)
class InputConfig(@JsonProperty("zookeeper") _zookeeper: String,
                  @JsonProperty("kafka") _kafka: KafkaConfig,
                  @JsonProperty("spark") _spark: SparkConfig,
                  @JsonProperty("druid") _druid: DruidConfig) extends Serializable {
  
  val ZOOKEEPER = _zookeeper
  val KAFKA = _kafka
  val SPARK = _spark
  val DRUID = _druid
}