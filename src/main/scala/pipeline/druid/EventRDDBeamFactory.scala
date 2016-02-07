package pipeline.druid

import com.metamx.tranquility.spark.BeamFactory
import com.metamx.tranquility.beam.Beam
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import io.druid.query.aggregation.CountAggregatorFactory
import org.joda.time.DateTime
import com.metamx.tranquility.druid._
import io.druid.granularity.QueryGranularity
import com.metamx.tranquility.beam.ClusteredBeamTuning
import com.metamx.common.Granularity
import org.joda.time.Period
import pipeline.config.InputConfig

class EventRDDBeamFactory(config: InputConfig) extends BeamFactory[Map[String, String]] {
  
  lazy val makeBeam: Beam[Map[String, String]] = {
    val curator = CuratorFrameworkFactory.newClient(
      config.DRUID.ZOOKEEPER,
      new BoundedExponentialBackoffRetry(config.DRUID.CURATOR_RETRY.BASE_SLEEP_MS, config.DRUID.CURATOR_RETRY.MAX_SLEEP_MS, config.DRUID.CURATOR_RETRY.RETRIES))
    curator.start()

    val aggregators = config.DRUID.ROLLUP.getAggregatorsSeq()
    val dimensionExclusions = config.DRUID.ROLLUP.getDimensionExclusionsSeq()
    val timestampFn = (message: Map[String, String]) => new DateTime(message.get(config.DRUID.TIMESTAMP_DIMENSION).get)

    DruidBeams
      .builder(timestampFn)
      .curator(curator)
      .discoveryPath(config.DRUID.DISCOVERY)
      .location(DruidLocation.create(config.DRUID.INDEXER, config.DRUID.DATASOURCE))
      .rollup(DruidRollup(SchemalessDruidDimensions(dimensionExclusions), aggregators, config.DRUID.ROLLUP.getQueryGranularity()))
      .tuning(ClusteredBeamTuning(
        segmentGranularity = config.DRUID.TUNING.getSegmentGranularity(),
        windowPeriod = new Period(config.DRUID.TUNING.WINDOW_PERIOD),
        partitions = config.DRUID.TUNING.PARTITIONS,
        replicants = config.DRUID.TUNING.REPLICANTS))
      .buildBeam()
  }
  
}