def mkDeltaTable(name: String, schema: String) = {
    import sys.process._
    // remove old tables
    s"hadoop fs -rm -r /tmp/delta-$name".!
    spark.sql(s"DROP TABLE IF EXISTS $name")

    // create table
    spark.sql(s"""
        CREATE TABLE $name ($schema)
        USING DELTA
        LOCATION '/tmp/delta-$name'
    """)
}

mkDeltaTable("w_time", """
    Id STRING,
    year INT,
    month INT,
    day INT
""")
mkDeltaTable("w_wind", """
    Wind_id STRING,
    Wind_min INT,
    Wind_max INT
""")
mkDeltaTable("w_temperature", """
    Temperature_id STRING,
    Temperature_min DOUBLE,
    Temperature_max DOUBLE
""")
mkDeltaTable("w_visibility", """
    Visibility_id STRING,
    Visibility_min DOUBLE,
    Visibility_max DOUBLE
""")
mkDeltaTable("w_location", """
    Id STRING,
    Country STRING,
    State STRING,
    Zipcode STRING
""")
mkDeltaTable("f_accident", """
    Date DATE,
    Location_id STRING,
    Wind_id STRING,
    Temperature_id STRING,
    Visibility_id STRING,
    Crossing BOOLEAN,
    Station BOOLEAN,
    Distance DOUBLE,
    Severity LONG,
    Number_of_accidents LONG
""")
