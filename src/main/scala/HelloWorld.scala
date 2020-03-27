import org.apache.spark.sql.{SaveMode, SparkSession}

object HelloWorld {
  def main(args: Array[String]): Unit = {
    print("Hello World")
    val spark = SparkSession
      .builder()
      .appName("redis-example")
      .master("local[*]")
      .config("spark.redis.host", "54.196.0.100")
      .config("spark.redis.port", "6379")
      .getOrCreate()
    val employeeData=spark.read
      .option("header",true)
      .csv("C:\\Users\\hp\\OneDrive\\Desktop\\employee.csv")


    employeeData  .write
      .format("org.apache.spark.sql.redis")
      .option("table", "person")
      .mode(SaveMode.Overwrite)
      .save()

    val loadedDf = spark.read
      .format("org.apache.spark.sql.redis")
      .option("table", "person")
      .load()
    loadedDf.printSchema()
    loadedDf.show()
  }
}
