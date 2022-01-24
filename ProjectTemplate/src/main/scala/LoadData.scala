import org.apache.spark.sql.SparkSession

object LoadData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("_PROJECT_TEMPLATE_")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
    import spark.implicits._

    // code

    spark.stop()
  }
}