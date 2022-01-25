import org.apache.spark.sql.SparkSession

object LoadData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("WindWarehouse")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
    import spark.implicits._

    val wind = spark.range(0, 240,5).map(x => (x.toString+"_"+ (x+5).toString,x.toInt,(x+5).toInt)).select($"_1".as("Wind_id"),$"_2".as("Wind_min"), $"_3".as("Wind_max") )
    wind.write.format("delta").mode("append").save("/tmp/delta-w_wind")

    spark.stop()
  }
}