import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.LocalDate

object LoadData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("TimeWarehouse")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val user = sc.sparkUser

    def loadCSV(name: String) : DataFrame = {
      val loc = s"""/user/$user/labs/us-accidents/$name"""
      spark.read.option("header", true).option("inferSchema", true).csv(loc)
    }

    val dateRange = (x: LocalDate, y: LocalDate) => Iterator.iterate(x) { _.plusDays(1) }.takeWhile(_.isBefore(y.plusDays(1))).toList
    val colDateRange = udf(dateRange)
    val getDate = udf((x: String) => x.substring(0, 10))

    val makeKey = udf((x: LocalDate) => x.toString)
    val getDay = udf((x: LocalDate) => x.getDayOfMonth)
    val getMonth = udf((x: LocalDate) => x.getMonthValue)
    val getYear = udf((x: LocalDate) => x.getYear)
    def warehouseLoadTime(file: String, dest: String) = {
      loadCSV(file)
        .withColumn("Date", explode(colDateRange(getDate($"Start_Time"), getDate($"End_Time"))))
        .withColumn("Id", makeKey($"Date"))
        .withColumn("year", getYear($"Date"))
        .withColumn("month", getMonth($"Date"))
        .withColumn("day", getDay($"Date"))
        .select("Id", "year", "month", "day")
        .distinct
        .write.format("delta").mode("append").save(dest)
    }
    val mainData = List("mainDataCentral.csv", "mainDataEastern.csv", "mainDataMountain.csv", "mainDataPacific.csv")
    for (file <- mainData) {
      println(s"Loading from $file")
      warehouseLoadTime(file, "/tmp/delta-w_time")
    }

    spark.stop()
  }
}