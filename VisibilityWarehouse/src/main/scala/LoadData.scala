import org.apache.spark.sql.SparkSession

object LoadData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("VisibilityWarehouse")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
    import spark.implicits._

    val visibility = spark.range(0, 20, 1).map(x => (x.toString+"_"+ (x+1).toString,x.toDouble,(x+1).toDouble)).select($"_1".as("Visibility_id"),$"_2".as("Visibility_min"), $"_3".as("Visibility_max") )
    visibility.write.format("delta").mode("append").save("/tmp/delta-w_visibility")

    spark.stop()
  }
}