package pipeline.config

import com.fasterxml.jackson.annotation.JsonProperty

@SerialVersionUID(1L)
class ConfigProperty(@JsonProperty("property") _property: String,
                       @JsonProperty("value") _value: String) extends Serializable {

  val PROPERTY = _property
  val VALUE = _value
}