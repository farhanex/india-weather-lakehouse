from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

STORAGE = "<storage>"
bronze = f"abfss://bronze@strgweatherde.dfs.core.windows.net/"
silver = f"abfss://silver@strgweatherde.dfs.core.windows.net/weather_hourly"


raw = (spark.read.option("multiLine", True).json(bronze + "*.json")
       .select("_metadata.file_path", "latitude", "longitude", "hourly"))

pat = r"/([A-Za-z]+)_(\d{4}-\d{2}-\d{2})\.json$"
flat = raw.select(
    "file_path", "latitude", "longitude",
    F.col("hourly.time").alias("time"),
    F.col("hourly.temperature_2m").alias("temperature_2m"),
    F.col("hourly.relative_humidity_2m").alias("relative_humidity_2m"),
    F.col("hourly.precipitation").alias("precipitation"),
    F.col("hourly.wind_speed_10m").alias("wind_speed_10m"))

df = (flat
  .withColumn("city", F.regexp_extract("file_path", pat, 1))
  .withColumn("ingest_date", F.to_date(F.regexp_extract("file_path", pat, 2)))
  .withColumn("r", F.explode(F.arrays_zip("time", "temperature_2m",
        "relative_humidity_2m", "precipitation", "wind_speed_10m")))
  .select("city", "latitude", "longitude", "ingest_date",
          F.to_timestamp("r.time").alias("reading_time"),
          F.col("r.temperature_2m").alias("temperature_c"),
          F.col("r.relative_humidity_2m").alias("humidity_pct"),
          F.col("r.precipitation").alias("precipitation_mm"),
          F.col("r.wind_speed_10m").alias("wind_kmh")))
display(df.limit(10))


w = Window.partitionBy("city", "reading_time").orderBy(F.col("ingest_date").desc())
clean = (df.dropna(subset=["city", "reading_time"])
           .withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn"))

bad = clean.filter("temperature_c < -50 OR temperature_c > 60").count()
assert bad == 0, f"{bad} rows have impossible temperatures"


if DeltaTable.isDeltaTable(spark, silver):
    (DeltaTable.forPath(spark, silver).alias("t")
       .merge(clean.alias("s"), "t.city = s.city AND t.reading_time = s.reading_time")
       .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
else:
    clean.write.format("delta").save(silver)

display(spark.read.format("delta").load(silver).groupBy("city").count())