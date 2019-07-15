import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class EventsCounterJob(spark: SparkSession) {

  def processStreams(events: DataFrame): DataFrame = {
    import spark.implicits._
    events
      .withColumn("timestamp", to_timestamp(from_unixtime('createtime)))
      .withWatermark("timestamp", "30 minutes")
      .groupBy(
        window('timestamp, "30 minutes"),
        'hotelid
      )
      .agg(
        sum(when('eventtype === "click", 1).otherwise(0)).as("eventtype_click_count"),
        sum(when('eventtype === "view", 1).otherwise(0)).as("eventtype_view_count")
      )
  }

}
