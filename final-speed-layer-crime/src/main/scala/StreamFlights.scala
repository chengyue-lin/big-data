import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes

object StreamFlights {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")
  
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val crimeHappenByWeather = hbaseConnection.getTable(TableName.valueOf("weather_crime_by_month"))
  val latestWeather = hbaseConnection.getTable(TableName.valueOf("chengyuel_final_weather_report"))
  
  def getLatestWeather(year: String, month: String, day: String) = {
      val result = latestWeather.get(new Get(Bytes.toBytes(year+month+day)))
      System.out.println(result.isEmpty())
      if(result.isEmpty())
        None
      else
        Some(WeatherReport(
              year,
              month,
              day,
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("fog"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("rain"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("snow"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("hail"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("thunder"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("tornado")))))
  }
  
  def incrementCrimeByDate(kfr : KafkaCrimeRecord) : String = {
    val maybeLatestWeather = getLatestWeather(kfr.year, kfr.month, kfr.day)
    if(maybeLatestWeather.isEmpty)
      return "No weather for date " + kfr.year + "/" + kfr.month + "/" + kfr.day;
    val latestWeather = maybeLatestWeather.get
    val inc = new Increment(Bytes.toBytes(kfr.year + kfr.month))
    if(latestWeather.clear) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("clear_total"), 1)
    }
    if(latestWeather.fog) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("fog_total"), 1)
    }
    if(latestWeather.rain) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("rain_total"), 1)
    }
    if(latestWeather.snow) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("snow_total"), 1)
    }
    if(latestWeather.hail) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("hail_total"), 1)
    }
    if(latestWeather.thunder) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("thunder_total"), 1)
    }
    if(latestWeather.tornado) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("tornado_total"), 1)
    }
    inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("total_crime"), 1)
    crimeHappenByWeather.increment(inc)
    return "Updated speed layer for crime at " + kfr.month + ", " + kfr.year
  }
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }
    
    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamFlights")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("chengyuel_final_crime")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);

    val kfrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[KafkaCrimeRecord]))

    // Update speed table    
    val processedFlights = kfrs.map(incrementCrimeByDate)
    processedFlights.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
