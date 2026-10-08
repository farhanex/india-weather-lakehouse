from pyspark.sql import functions as F

STORAGE = "strgweatherde"
silver = f"abfss://silver@strgweatherde.dfs.core.windows.net/weather_hourly"
gold = f"abfss://gold@strgweatherde.dfs.core.windows.net/"

s = spark.read.format("delta").load(silver)

dim_city = s.select("city", "latitude", "longitude").dropDuplicates(["city"])

fact_daily = (s.withColumn("date", F.to_date("reading_time"))
  .groupBy("city" , "date")
  .agg(F.round(F.avg("temperature_c"), 1).alias("avg_temp_c"),
       F.max("temperature_c").alias("max_temp_c"),
       F.min("temperature_c").alias("min_temp_c"),
       F.round(F.avg("humidity_pct"), 1).alias("avg_humidity_pct"),
       F.round(F.sum("precipitation_mm"), 1).alias("total_rain_mm"),
       F.round(F.avg("wind_kmh"), 1).alias("avg_wind_kmh")))

dim_city.write.format("delta").mode("overwrite").save(gold + "dim_city")
fact_daily.write.format("delta").mode("overwrite").save(gold + "fact_daily_weather")
display(fact_daily)

