import org.apache.spark.sql.SparkSession

object LoadData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("TemperatureWarehouse")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
    import spark.implicits._

    val temperature = spark.range(-128, 128,8).map(x => (x.toString+"_"+ (x+8).toString,x.toDouble,(x+8).toDouble)).select($"_1".as("Temperature_id"),$"_2".as("Temperature_min"), $"_3".as("Temperature_max") )
    temperature.write.format("delta").mode("append").save("/tmp/delta-w_temperature")

    spark.stop()
  }
}