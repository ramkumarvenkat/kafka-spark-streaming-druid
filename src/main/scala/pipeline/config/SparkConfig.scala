package pipeline.config

import com.fasterxml.jackson.annotation.JsonProperty
import java.util.List

@SerialVersionUID(1L)
class SparkConfig(@JsonProperty("appname") _appname: String,
                  @JsonProperty("master") _master: String,
                  @JsonProperty("durationS") _durationS: Int,
                  @JsonProperty("checkpoint") _checkpoint: String,
                  @JsonProperty("kafka") _kafka: SparkKafkaConfig) extends Serializable {
  
  val APPNAME = _appname
  val MASTER = _master
  val DURATION_S = _durationS
  val CHECKPOINT = _checkpoint
  val KAFKA = _kafka
}

@SerialVersionUID(1L)
class SparkKafkaConfig(@JsonProperty("group") _group: String,
                  @JsonProperty("properties") _properties: List[ConfigProperty]) extends Serializable {

  val GROUP = _group
  val PROPERTIES = _properties
}