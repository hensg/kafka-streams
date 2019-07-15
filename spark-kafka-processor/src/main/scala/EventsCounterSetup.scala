import java.io.PrintWriter
import java.time.Instant

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object EventsCounterSetup {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("henrique-playground")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inputKafka = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "connect-test")
      .load()
      .selectExpr("cast(value as string) as value")
      .withColumn("value", regexp_extract('value, "\\d+,\\.+,(click|view)", 0))
      .as[String]
      .map(_.split(","))
      .withColumn("createtime", 'value.getItem(0))
      .withColumn("hotelid", 'value.getItem(1).cast("int"))
      .withColumn("eventtype", 'value.getItem(2))

    val eventsInput = spark
      .readStream
      .option("sep", ",")
      .schema(new StructType()
        .add("createtime", "string")
        .add("hotelid", "int")
        .add("eventtype", "string")
      )
      .csv(this.getClass.getResource("/events").getPath)

    //uing CSV for development
    new EventsCounterJob(spark)
      .processStreams(eventsInput)
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination(30000)

    /*======== Instruction 0
    +------------------------------------------+-------+---------------------+--------------------+
    |window                                    |hotelid|eventtype_click_count|eventtype_view_count|
    +------------------------------------------+-------+---------------------+--------------------+
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|37     |4                    |9                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|72     |5                    |3                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|55     |7                    |6                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|46     |8                    |4                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|75     |5                    |5                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|15     |3                    |5                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|23     |5                    |2                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|76     |2                    |2                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|18     |3                    |5                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|57     |1                    |4                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|70     |4                    |6                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|81     |7                    |9                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|61     |6                    |7                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|63     |3                    |5                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|38     |7                    |3                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|27     |3                    |5                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|94     |4                    |5                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|1      |5                    |4                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|97     |3                    |8                   |
    |[2019-07-09 18:30:00, 2019-07-09 19:00:00]|5      |8                    |6                   |
    +------------------------------------------+-------+---------------------+--------------------+
    only showing top 20 rows
    ======*/
  }

  //FIXME: implement tests instead of "running local" for tests
  def generateEventsData(): Unit = {
    val pw = new PrintWriter("eventsdata")
    val r = scala.util.Random
    val eventTypes = Array("click", "view")
    for (i <- 0 to 1000) {
      val time = Instant.now.getEpochSecond
      val hotelId = r.nextInt(100)
      val eventType = eventTypes(r.nextInt(2))
      pw.write(s"$time,$hotelId,$eventType\n")
    }
    pw.close()
  }

}
