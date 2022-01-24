import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

object LoadData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("AccidentWarehouse")
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

    val weather_pattern = """On (.*?) .*airport (.*?) .*Temperature \(F\): (.*?),.*Visibility \(miles\): (.*?),.*Wind Speed \(mph\): (.*?),.*""".r
    def tryDouble(v: String) : Option[Double] = {
      if (v == "~")
        null
      else
        Option(v.toDouble)
    }
    def parseWeather(line: String) = {
      val weather_pattern(date, airport, temp, wind, visibility) = line
      (date, airport, tryDouble(temp), tryDouble(wind), tryDouble(visibility))
    }

    val weatherData = sc.textFile(s"/user/$user/labs/us-accidents/weather.txt").map(parseWeather)
    val weather = spark.createDataFrame(weatherData).toDF("Date", "Airport_code", "Temperature", "Wind_speed", "Visibility")

    val dateRange = (x: LocalDate, y: LocalDate) => Iterator.iterate(x) { _.plusDays(1) }.takeWhile(_.isBefore(y.plusDays(1))).toList
    val colDateRange = udf(dateRange)
    val getDate = udf((x: String) => x.substring(0, 10))

    val wind = spark.read.format("delta").load("/tmp/delta-w_wind")
    val temperature = spark.read.format("delta").load("/tmp/delta-w_temperature")
    val visibility = spark.read.format("delta").load("/tmp/delta-w_visibility")
    val location = spark.read.format("delta").load("/tmp/delta-w_location").withColumnRenamed("ID", "location_id")
    def warehouseLoadAccident(file: String, dest: String, weather: DataFrame, country: String) = {
      loadCSV(file)
        .withColumnRenamed("Distance(mi)", "Distance")
        .withColumn("Unit", expr("1"))
        .withColumn("Date", explode(colDateRange(getDate($"Start_Time"), getDate($"End_Time"))))
        .join(weather, Seq("Date", "Airport_code"))
        .join(wind)
        .where($"Wind_speed" >= $"Wind_min" && $"Wind_speed" < $"Wind_max")
        .join(temperature)
        .where($"Temperature" >= $"Temperature_min" && $"Temperature" < $"Temperature_max")
        .join(visibility)
        .where($"Visibility" >= $"Visibility_min" && $"Visibility" < $"Visibility_max")
        .withColumn("Country", expr(s""""$country""""))
        .join(location, Seq("Country", "Zipcode"))
        .select("Location_id", "Date", "Wind_id", "Temperature_id", "Visibility_id", "Crossing", "Station", "Distance", "Severity", "Unit")
        .groupBy("Location_id", "Wind_id", "Temperature_id", "Visibility_id", "Date", "Crossing", "Station")
        .agg(sum("Unit").as("Number_of_accidents"), sum("Distance").as("Distance"), sum("Severity").as("Severity"))
        .withColumnRenamed("Unit", "Number_of_accidents")
        .write.format("delta").option("mergeSchema", "true").mode("append").save(dest)
    }
    val mainData = List("mainDataCentral.csv", "mainDataEastern.csv", "mainDataMountain.csv", "mainDataPacific.csv")

    for (file <- mainData) {
      println(s"Loading from $file")
      warehouseLoadAccident(file, "/tmp/delta-f_accident", weather, "US")
    }

    spark.stop()
  }
}