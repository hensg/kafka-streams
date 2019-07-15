import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class EventsCounterCityJob(spark: SparkSession) {

  def processStreams(events: DataFrame, location: DataFrame): DataFrame = {
    import spark.implicits._

    //FIXME: review it -- not ok
    events
      .join(location.select("hotelid", "city"), "hotelid")
      .withColumn("timestamp", to_timestamp(from_unixtime('createtime)))
      .withWatermark("timestamp", "30 minutes")
      .groupBy(
        window('timestamp, "30 minutes"),
        'city
      )
      .agg(
        sum(when('eventtype === "click", 1).otherwise(0)).as("eventtype_click_count"),
        sum(when('eventtype === "view", 1).otherwise(0)).as("eventtype_view_count")
      )
  }

}
