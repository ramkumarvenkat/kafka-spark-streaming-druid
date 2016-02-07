package pipeline.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.metamx.common.Granularity
import java.util.List
import scala.collection.JavaConversions._
import io.druid.query.aggregation.LongSumAggregatorFactory
import io.druid.granularity.QueryGranularity

@SerialVersionUID(1L)
class DruidConfig(@JsonProperty("zookeeper") _zookeeper: String,
                  @JsonProperty("indexer") _indexer: String,
                  @JsonProperty("discovery") _discovery: String,
                  @JsonProperty("datasource") _datasource: String,
                  @JsonProperty("curator-retry") _curatorRetry: DruidCuratorRetryConfig,
                  @JsonProperty("tuning") _tuning: DruidTuningConfig,
                  @JsonProperty("timestamp-dimension") _timestampDimension: String,
                  @JsonProperty("rollup") _rollup: DruidRollupConfig) extends Serializable {
  
  val ZOOKEEPER = _zookeeper
  val INDEXER = _indexer
  val DISCOVERY = _discovery
  val DATASOURCE = _datasource
  val CURATOR_RETRY = _curatorRetry
  val TUNING = _tuning
  val TIMESTAMP_DIMENSION = _timestampDimension
  val ROLLUP = _rollup
}

@SerialVersionUID(1L)
class DruidCuratorRetryConfig(@JsonProperty("baseSleepMs") _baseSleepMs: Int,
                       @JsonProperty("maxSleepMs") _maxSleepMs: Int,
                       @JsonProperty("retries") _retries: Int) extends Serializable {

  val BASE_SLEEP_MS = _baseSleepMs
  val MAX_SLEEP_MS = _maxSleepMs
  val RETRIES = _retries
}

@SerialVersionUID(1L)
class DruidTuningConfig(@JsonProperty("segmentGranularity") _segmentGranularity: String,
                        @JsonProperty("windowPeriod") _windowPeriod: String,
                        @JsonProperty("partitions") _partitions: Int,
                        @JsonProperty("replicants") _replicants: Int) extends Serializable {

  val SEGMENT_GRANULARITY = _segmentGranularity
  val WINDOW_PERIOD = _windowPeriod
  val PARTITIONS = _partitions
  val REPLICANTS = _replicants
  
  def getSegmentGranularity() = {
    SEGMENT_GRANULARITY match {
      case "HOUR" => Granularity.HOUR
      case "MINUTE" => Granularity.MINUTE
      case _ => Granularity.HOUR
    }
  }
}

@SerialVersionUID(1L)
class DruidRollupConfig(@JsonProperty("granularity") _granularity: String,
                       @JsonProperty("aggregators") _aggregators: List[DruidAggregator],
                       @JsonProperty("dimension-exclusions") _dimensionExclusions: List[String]) extends Serializable {

  val GRANULARITY = _granularity
  val AGGREGATORS = _aggregators
  val DIMENSION_EXCLUSIONS = _dimensionExclusions
  
  def getAggregatorsSeq() = {
    AGGREGATORS.toList.map(aggregator => {
      aggregator.TYPE match {
        case "LONGSUM" => new LongSumAggregatorFactory(aggregator.NAME, aggregator.FIELD_NAME)
        case _ => new LongSumAggregatorFactory(aggregator.NAME, aggregator.FIELD_NAME)
      }
    }).toSeq
  }
  
  def getDimensionExclusionsSeq() = {
    DIMENSION_EXCLUSIONS.toSeq
  }
  
  def getQueryGranularity() = {
    GRANULARITY match {
      case "MINUTE" => QueryGranularity.MINUTE
      case "HOUR" => QueryGranularity.HOUR
      case _ => QueryGranularity.MINUTE
    }
  }
}

@SerialVersionUID(1L)
class DruidAggregator(@JsonProperty("type") _type: String,
                       @JsonProperty("name") _name: String,
                       @JsonProperty("field-name") _fieldName: String) extends Serializable {

  val TYPE = _type
  val NAME = _name
  val FIELD_NAME = _fieldName
}