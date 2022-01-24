import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{monotonically_increasing_id, udf}

object LoadData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("LocationWarehouse")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    import spark.implicits._

    def loadCSV(name: String) : DataFrame = {
      val loc = s"""/user/hadoop/labs/us-accidents/$name"""
      spark.read.option("header", true).option("inferSchema", true).csv(loc)
    }

    def warehouseLoadLocation(file: String, dest: String, name: String) = {
      val addPrefix = udf((x: Long) => name + x.toString)
      loadCSV(file)
        .withColumn("Id", addPrefix(monotonically_increasing_id()))
        .withColumn("Country", $"Country")
        .withColumn("State", $"State")
        .withColumn("City", $"City")
        .withColumn("Zipcode", $"Zipcode")
        .select("Id", "Country", "State", "City", "Zipcode")
        .write.format("delta").option("mergeSchema", "true").mode("append").save(dest)
    }
    val geoData = List("geoDataCentral.csv", "geoDataEastern.csv", "geoDataMountain.csv", "geoDataPacific.csv")
    for (file <- geoData) {
      println(s"Loading from $file")
      warehouseLoadLocation(file, "/tmp/delta-w_location", file.substring(7, file.indexOf(".")))
    }

    spark.stop()
  }
}