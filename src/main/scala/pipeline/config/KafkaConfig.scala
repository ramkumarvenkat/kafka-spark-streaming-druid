package pipeline.config

import com.fasterxml.jackson.annotation.JsonProperty

@SerialVersionUID(1L)
class KafkaConfig(@JsonProperty("topic") _topic: String,
                  @JsonProperty("broker") _broker: String) extends Serializable {
  
  val TOPIC = _topic
  val BROKER = _broker
}