import java.io.PrintWriter
import java.time.Instant

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object EventsCounterCitySetup {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("henrique-playground")
      .master("local[*]")
      .getOrCreate()

    val eventsInput = spark
      .readStream
      .option("sep", ",")
      .schema(new StructType()
        .add("createtime", "string")
        .add("hotelid", "int")
        .add("eventtype", "string")
      )
      .csv(this.getClass.getResource("/events").getPath)

    val locationInput = spark
      .readStream
      .option("sep", ",")
      .schema(new StructType()
        .add("createtime", "string")
        .add("hotelid", "int")
        .add("city", "string")
      )
      .csv(this.getClass.getResource("/location").getPath)

    //uing CSV for development
    new EventsCounterCityJob(spark)
      .processStreams(eventsInput, locationInput)
      .writeStream
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination(30000)

    /*======== Instruction 0
    only showing top 20 rows
    ======*/
  }

  //FIXME: implement tests instead of "running local"
  def generateCities(): Unit = {
    val pw = new PrintWriter("locationdata")
    val r = scala.util.Random
    val cities = Array("Tokyo", "LA", "Rio de Janeiro",  "Cape Town")
    for (i <- 0 to 100) {
      val time = Instant.now.getEpochSecond
      val hotelId = i
      val city = cities(r.nextInt(4))
      pw.write(s"$time,$hotelId,$city\n")
    }
    pw.close()
  }
}
