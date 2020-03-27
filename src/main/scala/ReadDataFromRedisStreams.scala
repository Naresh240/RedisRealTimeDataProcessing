import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object ReadDataFromRedisStreams {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("redis-example")
      .master("local[*]")
      .config("spark.redis.host", "100.24.49.116")
      .config("spark.redis.port", "6379")
      .getOrCreate()
    val clicks = spark
      .readStream
      .format("redis")
      .option("stream.keys", "clicks")
      .schema(StructType(Array(
        StructField("asset", StringType),
        StructField("cost", LongType)
      )))
      .load()
    val byasset = clicks.groupBy("asset").count

    val clickWriter: ClickForeachWriter =
      new ClickForeachWriter("100.24.49.116", "6379")

    val query = byasset
      .writeStream
      .outputMode("update")
      .foreach(clickWriter)
      .start()

    query.awaitTermination()
  }
}
