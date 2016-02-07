package pipeline

import java.io.FileReader
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import pipeline.config.InputConfig
import pipeline.spark.SparkStreamingTasks

object Runner {

  def main(args: Array[String]): Unit = {
    
    if(args.length < 1) {
      System.exit(1)
    }
    
    val reader = new FileReader(args(0))
    val mapper = new ObjectMapper(new YAMLFactory())
    val config: InputConfig = mapper.readValue(reader, classOf[InputConfig])
    
    val sparkContext = SparkStreamingTasks.startSparkContext(config)
    sparkContext.start()
    sparkContext.awaitTermination()
  }
  
}
